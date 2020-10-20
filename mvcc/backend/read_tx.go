// Copyright 2017 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"bytes"
	"math"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// safeRangeBucket is a hack to avoid inadvertently reading duplicate keys;
// safeRangeBucket是一种可以避免无意中读取重复密钥的黑科技；
// overwrites on a bucket should only fetch with limit=1, but safeRangeBucket
// is known to never overwrite any key so range is safe.
// 在 bucket 上进行的 重写，只有limit=1的时候进行提取，但是 safeRangeBucket 永不会重写任何 key ，所以 range 是安全的。
var safeRangeBucket = []byte("key")

type ReadTx interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()

	UnsafeRange(bucketName []byte, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte)
	UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error
}

// Base type for readTx and concurrentReadTx to eliminate duplicate functions between these
// readTx和parallelReadTx的基本类型，以消除它们之间的重复功能
// 这是 readTx和concurrentReadTx的基本类型，可以看下面这两个的实现。就是定义了公用属性和公用函数。
// 就是对bolt的封装，保存了一个bucket的map，当做缓存，别每次用bucket的时候都创建呗。还有bolt.Tx的一个属性，然后就是对他们的buf，和锁
// 它的一些读取操作，基本都是从buf取，取不到去bucket中取
type baseReadTx struct {
	// mu protects accesses to the txReadBuffer
	// txReadBuffer 的锁
	mu  sync.RWMutex
	buf txReadBuffer

	// TODO: group and encapsulate {txMu, tx, buckets, txWg}, as they share the same lifecycle.
	// txMu protects accesses to buckets and tx on Range requests.
	// txMu 在Range请求的时候，保护 buckets和tx
	txMu    *sync.RWMutex
	tx      *bolt.Tx
	buckets map[string]*bolt.Bucket
	// txWg protects tx from being rolled back at the end of a batch interval until all reads using this tx are done.
	// txWg 用来在 batch 操作中 回滚的时候保护tx，知道所有的用该tx读操作结束
	txWg *sync.WaitGroup
}

// bucketName 类似 数据库的库名， visitor 是具体要执行的操作。
func (baseReadTx *baseReadTx) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	dups := make(map[string]struct{})
	getDups := func(k, v []byte) error {
		dups[string(k)] = struct{}{}
		return nil
	}
	visitNoDup := func(k, v []byte) error {
		if _, ok := dups[string(k)]; ok {
			return nil
		}
		return visitor(k, v)
	}
	// 找到buf中有的，存入dup是中
	if err := baseReadTx.buf.ForEach(bucketName, getDups); err != nil {
		return err
	}
	// buf 中没有的 去boltDB去ForEach，然后执行visitor
	baseReadTx.txMu.Lock()
	err := unsafeForEach(baseReadTx.tx, bucketName, visitNoDup)
	baseReadTx.txMu.Unlock()
	if err != nil {
		return err
	}
	// buf 中有的，直接在buf中取，并调用。
	return baseReadTx.buf.ForEach(bucketName, visitor)
}

// bucketName 数据库名  key，endKey是范围，limit是最大返回的数量
func (baseReadTx *baseReadTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if endKey == nil {
		// forbid duplicates for single keys
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	if limit > 1 && !bytes.Equal(bucketName, safeRangeBucket) {
		panic("do not use unsafeRange on non-keys bucket")
	}
	keys, vals := baseReadTx.buf.Range(bucketName, key, endKey, limit)
	if int64(len(keys)) == limit {
		// 如果buf中已经找到了限定数量的数据，就直接返回
		return keys, vals
	}

	// find/cache bucket
	// 找到并缓存bucket
	bn := string(bucketName)
	baseReadTx.txMu.RLock()
	bucket, ok := baseReadTx.buckets[bn]
	baseReadTx.txMu.RUnlock()
	lockHeld := false
	if !ok {
		baseReadTx.txMu.Lock()
		lockHeld = true
		bucket = baseReadTx.tx.Bucket(bucketName)
		baseReadTx.buckets[bn] = bucket
	}

	// ignore missing bucket since may have been created in this batch
	// 忽略丢失的存储桶，因为可能是在此批次中创建的
	if bucket == nil {
		if lockHeld {
			baseReadTx.txMu.Unlock()
		}
		return keys, vals
	}
	if !lockHeld {
		baseReadTx.txMu.Lock()
		lockHeld = true
	}
	c := bucket.Cursor()
	baseReadTx.txMu.Unlock()

	// 去 boltDB 找，并追加返回
	k2, v2 := unsafeRange(c, key, endKey, limit-int64(len(keys)))
	return append(k2, keys...), append(v2, vals...)
}

type readTx struct {
	baseReadTx
}

func (rt *readTx) Lock()    { rt.mu.Lock() }
func (rt *readTx) Unlock()  { rt.mu.Unlock() }
func (rt *readTx) RLock()   { rt.mu.RLock() }
func (rt *readTx) RUnlock() { rt.mu.RUnlock() }

func (rt *readTx) reset() {
	rt.buf.reset()
	rt.buckets = make(map[string]*bolt.Bucket)
	rt.tx = nil
	rt.txWg = new(sync.WaitGroup)
}

type concurrentReadTx struct {
	baseReadTx
}

func (rt *concurrentReadTx) Lock()   {}
func (rt *concurrentReadTx) Unlock() {}

// RLock is no-op. concurrentReadTx does not need to be locked after it is created.
func (rt *concurrentReadTx) RLock() {}

// RUnlock signals the end of concurrentReadTx.
func (rt *concurrentReadTx) RUnlock() { rt.txWg.Done() }
