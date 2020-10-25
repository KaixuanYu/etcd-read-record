// Copyright 2015 The etcd Authors
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

package cindex

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"go.etcd.io/etcd/v3/mvcc/backend"
)

var (
	metaBucketName = []byte("meta")

	consistentIndexKeyName = []byte("consistent_index")
)

// 这玩意单独开了个 bucket = meta ； key = consistent_index ； value 应该是一个 uint64 的数字。

// ConsistentIndexer is an interface that wraps the Get/Set/Save method for consistentIndex.
// ConsistentIndexer 是一个封装 对consistentIndex 的 Get/Set/Save 操作的接口
type ConsistentIndexer interface {

	// ConsistentIndex returns the consistent index of current executing entry.
	// ConsistentIndex 返回当前执行的entry的一致性 index
	ConsistentIndex() uint64

	// SetConsistentIndex set the consistent index of current executing entry.
	// SetConsistentIndex 设置一个当前执行的entry的一致性 index
	SetConsistentIndex(v uint64)

	// UnsafeSave must be called holding the lock on the tx.
	// It saves consistentIndex to the underlying stable storage.
	// UnsafeSave 必须在给tx加锁的情况下调用
	// 它 保存 一致性 index 到 底层的永久储存，应该就是存到boltDB吧。
	UnsafeSave(tx backend.BatchTx)

	// SetBatchTx set the available backend.BatchTx for ConsistentIndexer.
	// SetBatchTx 给 ConsistentIndexer 设置可用的 backend.BatchTx
	SetBatchTx(tx backend.BatchTx)
}

// consistentIndex implements the ConsistentIndexer interface.
type consistentIndex struct {
	tx backend.BatchTx
	// consistentIndex represents the offset of an entry in a consistent replica log.
	// consistentIndex 表示 一致性副本日志中entry的偏移量
	// it caches the "consistent_index" key's value. Accessed
	// through atomics so must be 64-bit aligned.
	// 它 缓存 consistent_index 键的值。通过原子访问，因此必须是64位对齐的。
	consistentIndex uint64
	// bytesBuf8 is a byte slice of length 8
	// to avoid a repetitive allocation in saveIndex.
	//bytesBuf8是长度为8的byte slice，以避免在saveIndex中重复分配。
	bytesBuf8 []byte
	mutex     sync.Mutex
}

func NewConsistentIndex(tx backend.BatchTx) ConsistentIndexer {
	return &consistentIndex{tx: tx, bytesBuf8: make([]byte, 8)} // 长度是8
}

// 从本身结构的consistentIndex拿，如果没有再从boltDB拿，拿到存在consistentIndex中。
func (ci *consistentIndex) ConsistentIndex() uint64 {

	if index := atomic.LoadUint64(&ci.consistentIndex); index > 0 {
		// 如果本身的consistentIndex变量保存的有，就直接返回。注意这里对 consistentIndex 是原子操作
		return index
	}
	ci.mutex.Lock() // 该锁只在这里和 SetBatchTx 出现。说明是保护 ci.tx [backend.BatchTx] 的
	defer ci.mutex.Unlock()
	ci.tx.Lock() //tx锁，同时只允许对一个tx操作
	defer ci.tx.Unlock()
	// 从 boltDB 拿
	_, vs := ci.tx.UnsafeRange(metaBucketName, consistentIndexKeyName, nil, 0)
	if len(vs) == 0 {
		return 0
	}
	v := binary.BigEndian.Uint64(vs[0])        //大端模式解
	atomic.StoreUint64(&ci.consistentIndex, v) //原子写
	return v
}

// 需要更新的操作调用一下呗就。
func (ci *consistentIndex) SetConsistentIndex(v uint64) {
	atomic.StoreUint64(&ci.consistentIndex, v) // 原子写
}

// 将 consistentIndex 成员变量放进 boltDB 中。注意这里是Unsafe，用的时候需要加锁。传入的tx，在外面要lock unlock。
func (ci *consistentIndex) UnsafeSave(tx backend.BatchTx) {
	bs := ci.bytesBuf8 // 这个ci.bytesBuf8 从一开始就只创建了一次，避免了重复创建
	binary.BigEndian.PutUint64(bs, ci.consistentIndex)
	// put the index into the underlying backend
	// tx has been locked in TxnBegin, so there is no need to lock it again
	tx.UnsafePut(metaBucketName, consistentIndexKeyName, bs)
}

func (ci *consistentIndex) SetBatchTx(tx backend.BatchTx) {
	ci.mutex.Lock()
	defer ci.mutex.Unlock()
	ci.tx = tx
}

func NewFakeConsistentIndex(index uint64) ConsistentIndexer {
	return &fakeConsistentIndex{index: index}
}

type fakeConsistentIndex struct{ index uint64 }

func (f *fakeConsistentIndex) ConsistentIndex() uint64 { return f.index }

func (f *fakeConsistentIndex) SetConsistentIndex(index uint64) {
	atomic.StoreUint64(&f.index, index)
}

func (f *fakeConsistentIndex) UnsafeSave(tx backend.BatchTx) {}
func (f *fakeConsistentIndex) SetBatchTx(tx backend.BatchTx) {}
