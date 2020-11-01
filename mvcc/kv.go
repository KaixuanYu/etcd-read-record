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

package mvcc

import (
	"go.etcd.io/etcd/v3/lease"
	"go.etcd.io/etcd/v3/mvcc/backend"
	"go.etcd.io/etcd/v3/mvcc/mvccpb"
	"go.etcd.io/etcd/v3/pkg/traceutil"
)

type RangeOptions struct {
	Limit int64
	Rev   int64
	Count bool
}

type RangeResult struct {
	KVs   []mvccpb.KeyValue
	Rev   int64
	Count int
}

type ReadView interface {
	// FirstRev returns the first KV revision at the time of opening the txn.
	// After a compaction, the first revision increases to the compaction
	// revision.
	// FirstRev 在打开txn的时候返回第一个KV revision
	// 压缩后，第一个 revision 会增加到 compaction revision
	FirstRev() int64

	// Rev returns the revision of the KV at the time of opening the txn.
	// Rev 在打开txn的时候返回KV的revision
	Rev() int64

	// Range gets the keys in the range at rangeRev.
	// Range 获取在rangeRev中的范围内的 keys
	// The returned rev is the current revision of the KV when the operation is executed.
	// 返回的revision是执行操作时KV的当前revision。
	// If rangeRev <=0, range gets the keys at currentRev. // 如果 rangeRev <= 0，range 获取当前revision的keys
	// If `end` is nil, the request returns the key. // 如果 end 是nil， 请求将返回 参数key
	// If `end` is not nil and not empty, it gets the keys in range [key, range_end).
	// If `end` is not nil and empty, it gets the keys greater than or equal to key.
	// Limit limits the number of keys returned.
	// If the required rev is compacted, ErrCompacted will be returned.
	// 如果请求的revision已经压缩了，ErrCompacted将被返回。
	Range(key, end []byte, ro RangeOptions) (r *RangeResult, err error)
}

// TxnRead represents a read-only transaction with operations that will not
// block other read transactions.
type TxnRead interface {
	ReadView
	// End marks the transaction is complete and ready to commit.
	End()
}

type WriteView interface {
	// DeleteRange deletes the given range from the store.
	// DeleteRange 删除给定范围内的store
	// A deleteRange increases the rev of the store if any key in the range exists.
	// 如果范围中的任何键存在，则deleteRange会增加存储的版本。
	//
	// The number of key deleted will be returned. 返回删除的key的数量
	// The returned rev is the current revision of the KV when the operation is executed.
	// It also generates one event for each key delete in the event history.
	// if the `end` is nil, deleteRange deletes the key.
	// if the `end` is not nil, deleteRange deletes the keys in range [key, range_end).
	DeleteRange(key, end []byte) (n, rev int64)

	// Put puts the given key, value into the store. Put also takes additional argument lease to
	// attach a lease to a key-value pair as meta-data. KV implementation does not validate the lease
	// id.
	// Put 将 给定的key value 储存进store中。Put 还需要额外的参数lease，来将lease附加到key-value对中，作为meta-data。
	// KV 实现中 没有对 lease id进行校验
	// A put also increases the rev of the store, and generates one event in the event history.
	// The returned rev is the current revision of the KV when the operation is executed.
	// put 还会提升store中的revision，并且在event历史中生成一个event。
	// 操作被执行后当前KV的 revision 将被返回
	Put(key, value []byte, lease lease.LeaseID) (rev int64)
}

// TxnWrite represents a transaction that can modify the store.
type TxnWrite interface {
	TxnRead
	WriteView
	// Changes gets the changes made since opening the write txn.
	// 更改获取自打开写入txn以来所做的更改
	Changes() []mvccpb.KeyValue
}

// txnReadWrite coerces a read txn to a write, panicking on any write operation.
// txnReadWrite 强迫 一个 read txn 变成 一个 write，写操作会panic
// 继承自 TxnRead 重写了write的函数，但是查询函数还是直接调用TxnRead的函数
type txnReadWrite struct{ TxnRead }

func (trw *txnReadWrite) DeleteRange(key, end []byte) (n, rev int64) { panic("unexpected DeleteRange") }
func (trw *txnReadWrite) Put(key, value []byte, lease lease.LeaseID) (rev int64) {
	panic("unexpected Put")
}
func (trw *txnReadWrite) Changes() []mvccpb.KeyValue { return nil }

func NewReadOnlyTxnWrite(txn TxnRead) TxnWrite { return &txnReadWrite{txn} }

type KV interface {
	ReadView
	WriteView

	// Read creates a read transaction.
	// Read 创建一个读事务
	Read(trace *traceutil.Trace) TxnRead

	// Write creates a write transaction.
	// Write 创建一个 写事务
	Write(trace *traceutil.Trace) TxnWrite

	// Hash computes the hash of the KV's backend.
	// Hash 计算 KV的backend的hash
	Hash() (hash uint32, revision int64, err error)

	// HashByRev computes the hash of all MVCC revisions up to a given revision.
	// HashByRev计算直到给定 revision 的所有 MVCC revision 的哈希值。
	HashByRev(rev int64) (hash uint32, revision int64, compactRev int64, err error)

	// Compact frees all superseded keys with revisions less than rev.
	// Compact释放revisions小于rev的所有被取代的键。
	Compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error)

	// Commit commits outstanding txns into the underlying backend.
	// Commit 将未完成的 txn 是提交给底层的 backend
	Commit()

	// Restore restores the KV store from a backend.
	// Restore 从 backend 还原 KV store
	Restore(b backend.Backend) error
	Close() error
}

// WatchableKV is a KV that can be watched.
// WatchableKV 是可以被watch的KV
type WatchableKV interface {
	KV
	Watchable
}

// Watchable is the interface that wraps the NewWatchStream function.
// Watchable 是封装 NewWatchStream 函数的 interface
type Watchable interface {
	// NewWatchStream returns a WatchStream that can be used to
	// watch events happened or happening on the KV.
	// NewWatchStream 返回 WatchStream，WatchStream 可以用来 watch 发生在KV上已经发生和正在发生的事件
	NewWatchStream() WatchStream
}

// ConsistentWatchableKV is a WatchableKV that understands the consistency
// algorithm and consistent index.
// If the consistent index of executing entry is not larger than the
// consistent index of ConsistentWatchableKV, all operations in
// this entry are skipped and return empty response.
// ConsistentWatchableKV是一个WatchableKV，它了解一致性算法和一致性索引。
//如果正在执行的条目的一致索引不大于ConsistentWatchableKV的一致索引，则跳过此条目中的所有操作并返回空响应。
type ConsistentWatchableKV interface {
	WatchableKV
	// ConsistentIndex returns the current consistent index of the KV.
	// ConsistentIndex 返回 KV 当前的一致性index
	ConsistentIndex() uint64
}
