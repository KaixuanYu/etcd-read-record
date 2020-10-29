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

package lease

import (
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"go.etcd.io/etcd/v3/etcdserver/cindex"
	pb "go.etcd.io/etcd/v3/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/v3/lease/leasepb"
	"go.etcd.io/etcd/v3/mvcc/backend"
	"go.uber.org/zap"
)

// NoLease is a special LeaseID representing the absence of a lease.
// NoLease是一个特殊的LeaseID，表示没有租约。
const NoLease = LeaseID(0)

// MaxLeaseTTL is the maximum lease TTL value
// MaxLeaseTTL是最大租赁TTL值
const MaxLeaseTTL = 9000000000

var (
	forever = time.Time{}

	leaseBucketName = []byte("lease") // lease 专门有一个 bucket （相当于有单独的一个表存）

	// maximum number of leases to revoke per second; configurable for tests
	//每秒撤销的最大租约数； 可配置用于测试
	leaseRevokeRate = 1000

	// maximum number of lease checkpoints recorded to the consensus log per second; configurable for tests
	leaseCheckpointRate = 1000

	// the default interval of lease checkpoint
	// 租约检查点的默认间隔
	defaultLeaseCheckpointInterval = 5 * time.Minute

	// maximum number of lease checkpoints to batch into a single consensus log entry
	// 批处理租约检查点的最大数量，可以成一个共识日志条目
	maxLeaseCheckpointBatchSize = 1000

	// the default interval to check if the expired lease is revoked
	// 检查到期的租约是否已撤销的默认间隔
	defaultExpiredleaseRetryInterval = 3 * time.Second

	ErrNotPrimary       = errors.New("not a primary lessor")
	ErrLeaseNotFound    = errors.New("lease not found")
	ErrLeaseExists      = errors.New("lease already exists")
	ErrLeaseTTLTooLarge = errors.New("too large lease TTL")
)

// TxnDelete is a TxnWrite that only permits deletes. Defined here
// to avoid circular dependency with mvcc.
// TxnDelete是仅允许删除的TxnWrite。 在此定义以避免与mvcc循环依赖。
type TxnDelete interface {
	DeleteRange(key, end []byte) (n, rev int64)
	End()
}

// RangeDeleter is a TxnDelete constructor.
// RangeDeleter是一个TxnDelete构造函数。
type RangeDeleter func() TxnDelete

// Checkpointer permits checkpointing of lease remaining TTLs to the consensus log. Defined here to
// avoid circular dependency with mvcc.
// Checkpointer允许将租赁剩余TTL的检查点指向共识日志。 在此定义以避免与mvcc循环依赖。
type Checkpointer func(ctx context.Context, lc *pb.LeaseCheckpointRequest)

type LeaseID int64

// Lessor owns leases. It can grant, revoke, renew and modify leases for lessee.
// lessor（出租人） 拥有 leases。 它可以 grant（授予） ， revoke（撤销）， renew（续订） 和 modify（修改） lessee（承租人） 的 leases（租赁）
type Lessor interface {
	// SetRangeDeleter lets the lessor create TxnDeletes to the store.
	// SetRangeDeleter 让 lessor 给 store 创建 TxnDeletes
	// Lessor deletes the items in the revoked or expired lease by creating
	// new TxnDeletes.
	// lessor通过创建新的TxnDeletes来删除已撤销或已过期租约中的item。
	SetRangeDeleter(rd RangeDeleter)

	SetCheckpointer(cp Checkpointer)

	// Grant grants a lease that expires at least after TTL seconds.
	// Grant会授予至少在TTL秒后到期的租约。
	Grant(id LeaseID, ttl int64) (*Lease, error)
	// Revoke revokes a lease with given ID. The item attached to the
	// given lease will be removed. If the ID does not exist, an error
	// will be returned.
	// Revoke 撤销 给定ID 的租约。 被附加到给定lease的item将被删除。如果ID不存在，一个错误将被返回
	Revoke(id LeaseID) error

	// Checkpoint applies the remainingTTL of a lease. The remainingTTL is used in Promote to set
	// the expiry of leases to less than the full TTL when possible.
	// 检查点将应用剩余的TTL。 剩余的TTL在Promote中用于将租约的到期时间设置为小于完整的TTL。
	Checkpoint(id LeaseID, remainingTTL int64) error

	// Attach attaches given leaseItem to the lease with given LeaseID.
	// If the lease does not exist, an error will be returned.
	//使用给定的LeaseID将给定的leaseItem附加到租约上。
	//如果租约不存在，将返回错误。
	Attach(id LeaseID, items []LeaseItem) error

	// GetLease returns LeaseID for given item.
	// If no lease found, NoLease value will be returned.
	// GetLease返回给定项目的LeaseID。
	//如果未找到租约，则将返回NoLease值。
	GetLease(item LeaseItem) LeaseID

	// Detach detaches given leaseItem from the lease with given LeaseID.
	// If the lease does not exist, an error will be returned.
	//分离从具有给定LeaseID的租约中分离给定的leaseItem。
	//如果租约不存在，将返回错误。
	Detach(id LeaseID, items []LeaseItem) error

	// Promote promotes the lessor to be the primary lessor. Primary lessor manages
	// the expiration and renew of leases.
	// Newly promoted lessor renew the TTL of all lease to extend + previous TTL.
	//Promote将出租人提升为主要出租人。 主要出租人管理租赁的到期和续期。
	//新提升的出租人更新所有租约的TTL以扩展+先前的TTL。
	// 还有主要出租人(primary lessor)和次要出租人？
	Promote(extend time.Duration)

	// Demote demotes the lessor from being the primary lessor.
	// 降级将出租人降级为主要出租人。
	Demote()

	// Renew renews a lease with given ID. It returns the renewed TTL. If the ID does not exist,
	// an error will be returned.
	// Renew续订具有给定ID的租约。 它返回更新的TTL。 如果ID不存在，将返回错误。
	Renew(id LeaseID) (int64, error)

	// Lookup gives the lease at a given lease id, if any
	//查找以给定的租约ID（如果有）提供租约
	Lookup(id LeaseID) *Lease

	// Leases lists all leases.
	// 返回所有的 租约
	Leases() []*Lease

	// ExpiredLeasesC returns a chan that is used to receive expired leases.
	// ExpiredLeasesC返回一个chan，用于接收过期的租约。
	ExpiredLeasesC() <-chan []*Lease

	// Recover recovers the lessor state from the given backend and RangeDeleter.
	// 恢复从给定的backend和RangeDeleter恢复出租人状态。
	Recover(b backend.Backend, rd RangeDeleter)

	// Stop stops the lessor for managing leases. The behavior of calling Stop multiple
	// times is undefined.
	// Stop停止出租方管理租赁。 多次调用Stop的行为是不确定的。
	Stop()
}

// lessor implements Lessor interface.
// TODO: use clockwork for testability.
type lessor struct {
	mu sync.RWMutex

	// demotec is set when the lessor is the primary.
	// 当 lessor 是 主 时， demotec 被设置。
	// demotec will be closed if the lessor is demoted.
	// 如果 lessor 被降级， demotec 将被关闭
	// 它的作用：
	// 1. 判断是不是primary lessor，就是当前的lessor是不是属于主节点
	// 2. 是个通道啊，降级的时候回被close，应该可以被通知到降级，但是没看到用的地方。
	demotec chan struct{}

	leaseMap map[LeaseID]*Lease // 这里存了一个 leaseID -> Lease 的映射
	// leaseExpiredNotifier 用来做过期用的
	// 1. 在创建lessor的时候该 leaseExpiredNotifier 被创建并且 Init
	// 2. 在Grant创建一个租约的时候，会被添加元素
	// 3. 在每500s一循环的死循环中，用来拿到最短过期时间的lease，如果该lease已经被revoke（删除），那么这里也Pop出来
	//    如果还在，但是已经过期了，就给该lease的time改成当前时间+3s，然后将lease放入expireC中通知其他节点删除。
	leaseExpiredNotifier *LeaseExpiredNotifier
	leaseCheckpointHeap  LeaseQueue

	// 比如 leaseMap中有{"lease1":[key1,key2],"lease2":[key3,key4]}
	// 那么 itemMap 中存了 {key1:lease1,key2:lease1,key3:lease2,key4:lease2}
	// 用法：1. 在创建lessor的时候make一个空map
	//      2. 在Attach操作的时候将存入 key->leaseID
	//      3. 在Detach操作的时候删除 key->leaseID。 在key的put和delete操作的时候，有执行Detach操作。
	//      4. GetLease()可以通过key取到相应的leaseID，以O(1)的复杂度。
	itemMap map[LeaseItem]LeaseID // 这里存了一个 key -> leaseID 的映射。存了所有的lease中item到lease的映射。

	// When a lease expires, the lessor will delete the
	// leased range (or key) by the RangeDeleter.
	// 租约到期时，出租人将通过RangeDeleter删除租借范围内的key（或单个key）。
	rd RangeDeleter

	// When a lease's deadline should be persisted to preserve the remaining TTL across leader
	// elections and restarts, the lessor will checkpoint the lease by the Checkpointer.
	// 当应保留租约的最后期限以保留领导者选举并重新启动时剩余的TTL时，出租人将由Checkpointer检查该租约。
	cp Checkpointer

	// backend to persist leases. We only persist lease ID and expiry for now.
	// The leased items can be recovered by iterating all the keys in kv.
	// backend 保存 leases。 目前只保存 leaseID和到期时间。
	// 租约的item可以被恢复，通过遍历kv中的keys
	b backend.Backend

	// minLeaseTTL is the minimum lease TTL that can be granted for a lease. Any
	// requests for shorter TTLs are extended to the minimum TTL.
	// minLeaseTTL 就是租约的最短过期时间。如果设置的租约的ttl晓宇它，那么会被扩展成 minLeaseTTL，也就是没有小于它的呗。
	minLeaseTTL int64

	expiredC chan []*Lease
	// stopC is a channel whose closure indicates that the lessor should be stopped.
	stopC chan struct{}
	// doneC is a channel whose closure indicates that the lessor is stopped.
	doneC chan struct{}

	lg *zap.Logger

	// Wait duration between lease checkpoints. checkpoints的时间间隔
	checkpointInterval time.Duration
	// the interval to check if the expired lease is revoked
	//检查过期的租约是否被撤销的间隔
	expiredLeaseRetryInterval time.Duration //默认3s
	ci                        cindex.ConsistentIndexer
}

type LessorConfig struct {
	MinLeaseTTL                int64
	CheckpointInterval         time.Duration
	ExpiredLeasesRetryInterval time.Duration
}

func NewLessor(lg *zap.Logger, b backend.Backend, cfg LessorConfig, ci cindex.ConsistentIndexer) Lessor {
	return newLessor(lg, b, cfg, ci)
}

func newLessor(lg *zap.Logger, b backend.Backend, cfg LessorConfig, ci cindex.ConsistentIndexer) *lessor {
	checkpointInterval := cfg.CheckpointInterval
	expiredLeaseRetryInterval := cfg.ExpiredLeasesRetryInterval
	if checkpointInterval == 0 {
		checkpointInterval = defaultLeaseCheckpointInterval
	}
	if expiredLeaseRetryInterval == 0 {
		expiredLeaseRetryInterval = defaultExpiredleaseRetryInterval
	}
	l := &lessor{
		leaseMap:                  make(map[LeaseID]*Lease),
		itemMap:                   make(map[LeaseItem]LeaseID),
		leaseExpiredNotifier:      newLeaseExpiredNotifier(),
		leaseCheckpointHeap:       make(LeaseQueue, 0),
		b:                         b,
		minLeaseTTL:               cfg.MinLeaseTTL,
		checkpointInterval:        checkpointInterval,
		expiredLeaseRetryInterval: expiredLeaseRetryInterval,
		// expiredC is a small buffered chan to avoid unnecessary blocking.
		// expiredC是一个小的缓冲通道，以避免不必要的阻塞。
		expiredC: make(chan []*Lease, 16),
		stopC:    make(chan struct{}),
		doneC:    make(chan struct{}),
		lg:       lg,
		ci:       ci,
	}
	l.initAndRecover()

	go l.runLoop()

	return l
}

// isPrimary indicates if this lessor is the primary lessor. The primary
// lessor manages lease expiration and renew.
// isPrimary 标识了 本lessor是否是主 lessor。 主 lessor 管理租约到期时间和续约。
//
// in etcd, raft leader is the primary. Thus there might be two primary
// leaders at the same time (raft allows concurrent leader but with different term)
// for at most a leader election timeout.
// 在etcd中， raft leader 是主。因此，可能同时有两个主要领导者（筏允许同时担任领导者，但任期不同），最多至一个领导者选举超时。

// The old primary leader cannot affect the correctness since its proposal has a
// smaller term and will not be committed.
//旧的主要领导者不能影响正确性，因为其提议的期限较短，因此不会付诸实施。
//
// * 看这意思 leader 节点上的 就是主 lessor
// TODO: raft follower do not forward lease management proposals. There might be a
// very small window (within second normally which depends on go scheduling) that
// a raft follow is the primary between the raft leader demotion and lessor demotion.
// Usually this should not be a problem. Lease should not be that sensitive to timing.
// raft follower 不转发 lease 管理 proposals。 可能会有很小一段时间（通常在2s内），raft follow 可能在 leader降级和lessor降级的时候是primary。
// 通常这应该不是问题。租约对时间不应太敏感。
func (le *lessor) isPrimary() bool {
	return le.demotec != nil
}

func (le *lessor) SetRangeDeleter(rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.rd = rd
}

// 这个函数给lessor设置一个checkpointer，该函数是在etcdServer中调用的，其赋值的函数就是给其他节点发送了一个LeaseCheckpoint类型的请求。
func (le *lessor) SetCheckpointer(cp Checkpointer) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.cp = cp
}

// 创建了一个租约啊就是。 在leaseExpiredNotifier中RegisterOrUpdate一个 LeaseWithTime。然后 scheduleCheckpointIfNeeded
func (le *lessor) Grant(id LeaseID, ttl int64) (*Lease, error) {
	if id == NoLease {
		return nil, ErrLeaseNotFound
	}

	if ttl > MaxLeaseTTL {
		return nil, ErrLeaseTTLTooLarge
	}

	// TODO: when lessor is under high load, it should give out lease
	// with longer TTL to reduce renew load.
	// TODO：出租人在高负载下，应提供更长的TTL租约以减少更新负载。
	l := &Lease{
		ID:      id,
		ttl:     ttl,
		itemSet: make(map[LeaseItem]struct{}),
		revokec: make(chan struct{}),
	}

	le.mu.Lock()
	defer le.mu.Unlock()

	if _, ok := le.leaseMap[id]; ok {
		return nil, ErrLeaseExists
	}

	if l.ttl < le.minLeaseTTL {
		l.ttl = le.minLeaseTTL
	}

	if le.isPrimary() {
		l.refresh(0) // 只有主节点设置了过期时间呀~
	} else {
		l.forever()
	}

	le.leaseMap[id] = l
	l.persistTo(le.b, le.ci)

	leaseTotalTTLs.Observe(float64(l.ttl))
	leaseGranted.Inc()

	if le.isPrimary() {
		item := &LeaseWithTime{id: l.ID, time: l.expiry.UnixNano()}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
		le.scheduleCheckpointIfNeeded(l)
	}

	return l, nil
}

// Revoke 撤销。 就删除了指定 leaseID 的 lease 的所有的 key. 也删除了DB中的该lease，和 lessor中的该lease
// 但是这里没有删除leaseExpiredNotifier中的，所以leaseExpiredNotifier只会在创建lessor时候的协程中删除。而且leaseExpiredNotifier.Unregister 也只有那个地方使用。
func (le *lessor) Revoke(id LeaseID) error {
	le.mu.Lock()

	l := le.leaseMap[id]
	if l == nil {
		le.mu.Unlock()
		return ErrLeaseNotFound
	}
	defer close(l.revokec)
	// unlock before doing external work
	le.mu.Unlock()

	if le.rd == nil {
		return nil
	}

	txn := le.rd()

	// sort keys so deletes are in same order among all members,
	// otherwise the backend hashes will be different
	//排序键，以便所有成员之间的删除顺序相同，
	//否则后端哈希值将有所不同
	keys := l.Keys() //返回lease 中 的itemSet中的所有key
	sort.StringSlice(keys).Sort()
	for _, key := range keys {
		txn.DeleteRange([]byte(key), nil) // 删除该租约下的所有的key
	}

	le.mu.Lock()
	defer le.mu.Unlock()
	delete(le.leaseMap, l.ID)
	// lease deletion needs to be in the same backend transaction with the
	// kv deletion. Or we might end up with not executing the revoke or not
	// deleting the keys if etcdserver fails in between.
	le.b.BatchTx().UnsafeDelete(leaseBucketName, int64ToBytes(int64(l.ID)))
	// if len(keys) > 0, txn.End() will call ci.UnsafeSave function.
	if le.ci != nil && len(keys) == 0 {
		le.ci.UnsafeSave(le.b.BatchTx())
	}

	txn.End()

	leaseRevoked.Inc()
	return nil
}

// 更新 lease 的 remainingTTL，并且调用scheduleCheckpointIfNeeded
func (le *lessor) Checkpoint(id LeaseID, remainingTTL int64) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	if l, ok := le.leaseMap[id]; ok {
		// when checkpointing, we only update the remainingTTL, Promote is responsible for applying this to lease expiry
		l.remainingTTL = remainingTTL // 只要这一个地方 更新乐 lessor.remainingTTL
		if le.isPrimary() {
			// schedule the next checkpoint as needed
			le.scheduleCheckpointIfNeeded(l)
		}
	}
	return nil
}

// Renew renews an existing lease. If the given lease does not exist or
// has expired, an error will be returned.
// Renew renews 一个已经存在的 lease。如果给定的lease不存在或者已经过期了，一个错误将会被返回。
// 好像就是更新了一下租约的过期时间。看着就是将lease中的remainingTTL置为ttl，然后作为主节点，并将这件事告知给其他的节点
func (le *lessor) Renew(id LeaseID) (int64, error) {
	le.mu.RLock()
	if !le.isPrimary() {
		// forward renew request to primary instead of returning error.
		le.mu.RUnlock()
		return -1, ErrNotPrimary
	}

	demotec := le.demotec

	l := le.leaseMap[id] //取出lease
	if l == nil {
		le.mu.RUnlock()
		return -1, ErrLeaseNotFound
	}
	// Clear remaining TTL when we renew if it is set 【renew的时候清除 remaining TTL】
	clearRemainingTTL := le.cp != nil && l.remainingTTL > 0

	le.mu.RUnlock()
	if l.expired() { // 如果已经过期
		select {
		// A expired lease might be pending for revoking or going through
		// quorum to be revoked. To be accurate, renew request must wait for the
		// deletion to complete.
		//到期的租约可能正在等待撤销或正在通过仲裁(quorum)被撤销。 为准确起见，续订请求必须等待删除完成。
		case <-l.revokec:
			return -1, ErrLeaseNotFound
		// The expired lease might fail to be revoked if the primary changes.
		// 过期的lease 可能因为主节点变化导致了撤销失败。
		// The caller will retry on ErrNotPrimary.
		case <-demotec:
			return -1, ErrNotPrimary
		case <-le.stopC:
			return -1, ErrNotPrimary
		}
	}

	// Clear remaining TTL when we renew if it is set // 如果设置了 remaining TTL ，那么当我们renew的时候清除remaining
	// By applying a RAFT entry only when the remainingTTL is already set, we limit the number
	// of RAFT entries written per lease to a max of 2 per checkpoint interval.
	// 通过仅在设置了剩余的TTL时才应用RAFT条目，我们将每个租约写入的RAFT条目的数量限制为每个检查点间隔最多2个。
	if clearRemainingTTL {
		le.cp(context.Background(), &pb.LeaseCheckpointRequest{Checkpoints: []*pb.LeaseCheckpoint{{ID: int64(l.ID), Remaining_TTL: 0}}})
	}

	le.mu.Lock()
	l.refresh(0)
	item := &LeaseWithTime{id: l.ID, time: l.expiry.UnixNano()}
	le.leaseExpiredNotifier.RegisterOrUpdate(item) //重新往堆中注册或者更新
	le.mu.Unlock()

	leaseRenewed.Inc()
	return l.ttl, nil
}

// 就是get一个lease，如果没有返回nil
func (le *lessor) Lookup(id LeaseID) *Lease {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.leaseMap[id]
}

// 返回leaseMap中的所有的lease。没加锁
func (le *lessor) unsafeLeases() []*Lease {
	leases := make([]*Lease, 0, len(le.leaseMap))
	for _, l := range le.leaseMap {
		leases = append(leases, l)
	}
	return leases
}

// 返回leaseMap中的所有的lease。加锁了，对剩余时间排序
func (le *lessor) Leases() []*Lease {
	le.mu.RLock()
	ls := le.unsafeLeases()
	le.mu.RUnlock()
	sort.Sort(leasesByExpiry(ls))
	return ls
}

//Promote 促进。todo 没看懂在干嘛
func (le *lessor) Promote(extend time.Duration) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.demotec = make(chan struct{})

	// refresh the expiries of all leases.
	// 刷新所有lease的到期时间
	for _, l := range le.leaseMap {
		l.refresh(extend)
		item := &LeaseWithTime{id: l.ID, time: l.expiry.UnixNano()}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
	}

	if len(le.leaseMap) < leaseRevokeRate {
		// no possibility of lease pile-up
		// 没有 lease 到期的可能性
		return
	}

	// adjust expiries in case of overlap 重叠时调整到期日
	leases := le.unsafeLeases()
	sort.Sort(leasesByExpiry(leases))

	baseWindow := leases[0].Remaining()
	nextWindow := baseWindow + time.Second
	expires := 0
	// have fewer expires than the total revoke rate so piled up leases
	// don't consume the entire revoke limit
	//到期时间少于总撤销率，因此堆积的租约不会消耗整个撤销限制
	targetExpiresPerSecond := (3 * leaseRevokeRate) / 4 // =750
	for _, l := range leases {
		remaining := l.Remaining()
		if remaining > nextWindow {
			baseWindow = remaining
			nextWindow = baseWindow + time.Second
			expires = 1
			continue
		}
		expires++
		if expires <= targetExpiresPerSecond {
			continue
		}
		//如果1s内过期的租约大于750，才走下面逻辑
		rateDelay := float64(time.Second) * (float64(expires) / float64(targetExpiresPerSecond)) // 这应该等于1s？ 751/750 s？
		// If leases are extended by n seconds, leases n seconds ahead of the
		// base window should be extended by only one second.
		// 如果将租约延长n秒，则在基本窗口之前的n秒应仅延长一秒钟。
		rateDelay -= float64(remaining - baseWindow)
		delay := time.Duration(rateDelay)
		nextWindow = baseWindow + delay // nextWindow = ？
		l.refresh(delay + extend)
		item := &LeaseWithTime{id: l.ID, time: l.expiry.UnixNano()}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
		le.scheduleCheckpointIfNeeded(l)
	}
}

type leasesByExpiry []*Lease

func (le leasesByExpiry) Len() int           { return len(le) }
func (le leasesByExpiry) Less(i, j int) bool { return le[i].Remaining() < le[j].Remaining() }
func (le leasesByExpiry) Swap(i, j int)      { le[i], le[j] = le[j], le[i] }

// 降级。将所有的lease的过期时间改成time.Time()，也就是永久。然后删除Checkpoints和过期通知着。close并删除lessor.demotec
func (le *lessor) Demote() {
	le.mu.Lock()
	defer le.mu.Unlock()

	// set the expiries of all leases to forever
	for _, l := range le.leaseMap {
		l.forever() // 就是给该lease一个time.Time{} 就是0的时间
	}

	le.clearScheduledLeasesCheckpoints() //删除检查点
	le.clearLeaseExpiredNotifier()       //删除过期通知

	if le.demotec != nil { //原来是主，删除 demotec
		close(le.demotec)
		le.demotec = nil
	}
}

// Attach attaches items to the lease with given ID. When the lease
// expires, the attached items will be automatically removed.
// If the given lease does not exist, an error will be returned.
// Attach 将 items 附加到 给定id的lease上。如果lease过期，被附加的 items将会被自动删除。如果给定的lease不存在。error将会被返回。
// 将 items 附加到 lease 和 lessor 上
func (le *lessor) Attach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	l := le.leaseMap[id]
	if l == nil {
		return ErrLeaseNotFound
	}

	l.mu.Lock()
	for _, it := range items {
		l.itemSet[it] = struct{}{} // lease 本身的itemSet存了一下
		le.itemMap[it] = id        // lessor 的itemMap也存了一下。
	}
	l.mu.Unlock()
	return nil
}

// 通过一个key获取到key附加到的Lease的LeaseID
func (le *lessor) GetLease(item LeaseItem) LeaseID {
	le.mu.RLock()
	id := le.itemMap[item] // 所以这个map就是为了方便这里用的？O(1)的复杂度来取？
	le.mu.RUnlock()
	return id
}

// Detach detaches items from the lease with given ID.
// If the given lease does not exist, an error will be returned.
// Detach 将item从给定ID的lease上删除
// 如果给定的lease不存在，会返回error
func (le *lessor) Detach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	l := le.leaseMap[id]
	if l == nil {
		return ErrLeaseNotFound
	}

	l.mu.Lock()
	for _, it := range items {
		delete(l.itemSet, it) // LeaseItem (也就是key) 就只有 lessor的itemMap和lease的itemSet有存。
		delete(le.itemMap, it)
	}
	l.mu.Unlock()
	return nil
}

// 重新给 backend， rangeDeleter ， leaseMap ， 和 itemMap 赋值。然后调用 initAndRecover()
func (le *lessor) Recover(b backend.Backend, rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.b = b
	le.rd = rd
	le.leaseMap = make(map[LeaseID]*Lease)
	le.itemMap = make(map[LeaseItem]LeaseID)
	le.initAndRecover()
}

//返回已经过期lease的一个chan，etcdServer中用到了。取出来通知给其他的raft节点lease过期。
func (le *lessor) ExpiredLeasesC() <-chan []*Lease {
	return le.expiredC
}

func (le *lessor) Stop() {
	close(le.stopC)
	<-le.doneC
}

// 每500ms往lessor.expiredC中放过期的lease，外面etcd server消费 expiredC，然后将该过期发送个其他raft 节点。
func (le *lessor) runLoop() {
	defer close(le.doneC)

	for {
		// 每500ms执行一次这两个函数
		le.revokeExpiredLeases()
		le.checkpointScheduledLeases()

		select {
		case <-time.After(500 * time.Millisecond):
		case <-le.stopC:
			return
		}
	}
}

// revokeExpiredLeases finds all leases past their expiry and sends them to expired channel for
// to be revoked.
//revokeExpiredLeases查找所有在其到期后的租约，并将其发送到过期的信道以进行撤消。
//  这个函数就是找到过期的lease，然后把过期lease放在了lessor.expiredC通道中，etcdServer中消费它，然后告诉其他节点这些lease过期了
func (le *lessor) revokeExpiredLeases() {
	var ls []*Lease

	// rate limit
	revokeLimit := leaseRevokeRate / 2 // leaseRevokeRate是每秒撤销的最大租约数。执行该函数的for死循环是500ms，所以这里除2

	le.mu.RLock()
	if le.isPrimary() { //也就是只有主节点才会执行以下动作。
		ls = le.findExpiredLeases(revokeLimit)
	}
	le.mu.RUnlock()

	if len(ls) != 0 {
		select {
		case <-le.stopC:
			return
		case le.expiredC <- ls:
		default:
			// the receiver of expiredC is probably busy handling
			// other stuff
			// let's try this next time after 500ms
			// expiredC的接收者可能正在忙于处理其他内容
			//让我们在500ms之后尝试下一次
		}
	}
}

// checkpointScheduledLeases finds all scheduled lease checkpoints that are due and
// submits them to the checkpointer to persist them to the consensus log.
// checkpointScheduledLeases查找所有到期的预定租赁检查点，并将其提交给检查点以将其持久化到共识日志中。
func (le *lessor) checkpointScheduledLeases() {
	var cps []*pb.LeaseCheckpoint

	// rate limit
	for i := 0; i < leaseCheckpointRate/2; i++ {
		le.mu.Lock()
		if le.isPrimary() {
			cps = le.findDueScheduledCheckpoints(maxLeaseCheckpointBatchSize)
		}
		le.mu.Unlock()

		if len(cps) != 0 {
			le.cp(context.Background(), &pb.LeaseCheckpointRequest{Checkpoints: cps})
		}
		if len(cps) < maxLeaseCheckpointBatchSize {
			return
		}
	}
}

// 清空 leaseCheckpointHeap 节点。
func (le *lessor) clearScheduledLeasesCheckpoints() {
	le.leaseCheckpointHeap = make(LeaseQueue, 0)
}

// 清空 leaseExpiredNotifier 节点
func (le *lessor) clearLeaseExpiredNotifier() {
	le.leaseExpiredNotifier = newLeaseExpiredNotifier()
}

// expireExists returns true if expiry items exist.
// expireExists 返回 true 如果 过期的item存在。
// It pops only when expiry item exists.
// 只有expiry item 存在才会 pop
// "next" is true, to indicate that it may exist in next attempt.
// “next”为true，表示它可能在下次尝试中存在
func (le *lessor) expireExists() (l *Lease, ok bool, next bool) {
	if le.leaseExpiredNotifier.Len() == 0 {
		return nil, false, false
	}

	item := le.leaseExpiredNotifier.Poll() // 堆排序数组中的第0个元素就是树的根，对于小顶堆来说就是最小的那个元素，所以下面如果l==nil，Unregister函数Pop出来没有问题。
	l = le.leaseMap[item.id]
	if l == nil {
		// lease has expired or been revoked
		// no need to revoke (nothing is expiry)
		le.leaseExpiredNotifier.Unregister() // O(log N) 在lessor中 lease已经不存在了，就在 Notifier 中删除了呗
		return nil, false, true
	}
	now := time.Now()
	if now.UnixNano() < item.time /* expiration time */ { // 这个判断条件是还没有过期。
		// Candidate expirations are caught up, reinsert this item
		// and no need to revoke (nothing is expiry)
		// 赶上候选者的过期时间，重新插入该项目，无需撤销（没有过期）
		// 没看懂这句注释的翻译
		return l, false, false
	}

	// recheck if revoke is complete after retry interval
	// 重试间隔后重新检查revoke是否完成
	item.time = now.Add(le.expiredLeaseRetryInterval).UnixNano()
	le.leaseExpiredNotifier.RegisterOrUpdate(item) //把已经过期了的item的过期时间改成当前时间+3s？
	return l, true, false
}

// findExpiredLeases loops leases in the leaseMap until reaching expired limit
// and returns the expired leases that needed to be revoked.
// findExpiredLeases在leaseMap中循环租约，直到达到过期限制
// 并返回需要撤销的过期租约。
func (le *lessor) findExpiredLeases(limit int) []*Lease {
	leases := make([]*Lease, 0, 16)

	for {
		l, ok, next := le.expireExists()
		if !ok && !next {
			break
		}
		if !ok {
			continue
		}
		if next {
			continue
		}

		if l.expired() {
			leases = append(leases, l)

			// reach expired limit
			if len(leases) == limit {
				break
			}
		}
	}

	return leases
}

func (le *lessor) scheduleCheckpointIfNeeded(lease *Lease) {
	if le.cp == nil {
		return
	}

	if lease.RemainingTTL() > int64(le.checkpointInterval.Seconds()) {
		if le.lg != nil {
			le.lg.Debug("Scheduling lease checkpoint",
				zap.Int64("leaseID", int64(lease.ID)),
				zap.Duration("intervalSeconds", le.checkpointInterval),
			)
		}
		heap.Push(&le.leaseCheckpointHeap, &LeaseWithTime{
			id:   lease.ID,
			time: time.Now().Add(le.checkpointInterval).UnixNano(),
		})
	}
}

func (le *lessor) findDueScheduledCheckpoints(checkpointLimit int) []*pb.LeaseCheckpoint {
	if le.cp == nil {
		return nil
	}

	now := time.Now()
	cps := []*pb.LeaseCheckpoint{}
	for le.leaseCheckpointHeap.Len() > 0 && len(cps) < checkpointLimit {
		lt := le.leaseCheckpointHeap[0]
		if lt.time /* next checkpoint time */ > now.UnixNano() {
			return cps
		}
		heap.Pop(&le.leaseCheckpointHeap)
		var l *Lease
		var ok bool
		if l, ok = le.leaseMap[lt.id]; !ok {
			continue
		}
		if !now.Before(l.expiry) {
			continue
		}
		remainingTTL := int64(math.Ceil(l.expiry.Sub(now).Seconds()))
		if remainingTTL >= l.ttl {
			continue
		}
		if le.lg != nil {
			le.lg.Debug("Checkpointing lease",
				zap.Int64("leaseID", int64(lt.id)),
				zap.Int64("remainingTTL", remainingTTL),
			)
		}
		cps = append(cps, &pb.LeaseCheckpoint{ID: int64(lt.id), Remaining_TTL: remainingTTL})
	}
	return cps
}

// 就是在启动的时候，刚初始化lessor的时候，创建leaseBucketName。如果已经有了，就从db中取出所有的lease，然后将lease赋值给 lessor.leaseMap
// 然后初始化leaseExpiredNotifier和leaseCheckpointHeap
func (le *lessor) initAndRecover() {
	tx := le.b.BatchTx()
	tx.Lock()

	tx.UnsafeCreateBucket(leaseBucketName)
	_, vs := tx.UnsafeRange(leaseBucketName, int64ToBytes(0), int64ToBytes(math.MaxInt64), 0)
	// TODO: copy vs and do decoding outside tx lock if lock contention becomes an issue.
	// 如果锁定争用成为问题，请复制vs并在tx锁定之外进行解码。
	// 这里遍历db中的所有lease，并将其赋值给lessor.leaseMap
	for i := range vs {
		var lpb leasepb.Lease
		err := lpb.Unmarshal(vs[i])
		if err != nil {
			tx.Unlock()
			panic("failed to unmarshal lease proto item")
		}
		ID := LeaseID(lpb.ID)
		if lpb.TTL < le.minLeaseTTL {
			lpb.TTL = le.minLeaseTTL
		}
		le.leaseMap[ID] = &Lease{
			ID:  ID,
			ttl: lpb.TTL,
			// itemSet will be filled in when recover key-value pairs
			// set expiry to forever, refresh when promoted
			itemSet: make(map[LeaseItem]struct{}),
			expiry:  forever,
			revokec: make(chan struct{}),
		}
	}
	le.leaseExpiredNotifier.Init()
	heap.Init(&le.leaseCheckpointHeap)
	tx.Unlock()

	le.b.ForceCommit()
}

type Lease struct {
	ID           LeaseID
	ttl          int64 // time to live of the lease in seconds [lease 的生存时间]
	remainingTTL int64 // remaining time to live in seconds, if zero valued it is considered unset and the full ttl should be used
	// 剩余的生存时间（以秒为单位），如果为零，则视为未设置，应使用完整的ttl

	// expiryMu protects concurrent accesses to expiry
	//Mu保护了对到期的并发访问
	expiryMu sync.RWMutex
	// expiry is time when lease should expire. no expiration when expiry.IsZero() is true
	// expiry是租约到期的时间。 如果 expiry.IsZero() 为 true，就没有过期时间。
	expiry time.Time // 刚创建 lease 的时候 ，该值给定的就是 ttl 的值

	// mu protects concurrent accesses to itemSet 对itemSet得并发访问控制
	mu      sync.RWMutex
	itemSet map[LeaseItem]struct{}
	revokec chan struct{} // 撤销的一个chan
}

//返回是否过期
func (l *Lease) expired() bool {
	return l.Remaining() <= 0
}

// 持久化到 backend。 boltDB ,存一个 bucket : lease  key:leaseID  value: leasepb.Lease 结构
func (l *Lease) persistTo(b backend.Backend, ci cindex.ConsistentIndexer) {
	key := int64ToBytes(int64(l.ID))

	lpb := leasepb.Lease{ID: int64(l.ID), TTL: l.ttl, RemainingTTL: l.remainingTTL}
	val, err := lpb.Marshal()
	if err != nil {
		panic("failed to marshal lease proto item")
	}

	b.BatchTx().Lock()
	b.BatchTx().UnsafePut(leaseBucketName, key, val) // bucket : lease  key:leaseID  value: leasepb.Lease
	if ci != nil {
		ci.UnsafeSave(b.BatchTx())
	}
	b.BatchTx().Unlock()
}

// TTL returns the TTL of the Lease.
func (l *Lease) TTL() int64 {
	return l.ttl
}

// RemainingTTL returns the last checkpointed remaining TTL of the lease.
// RemainintTTL 返回最后一次checkpointed的remainingTTL
// TODO(jpbetz): do not expose this utility method
func (l *Lease) RemainingTTL() int64 {
	if l.remainingTTL > 0 {
		return l.remainingTTL
	}
	return l.ttl
}

// refresh refreshes the expiry of the lease.
// refresh 更新 lease 的 expiry
func (l *Lease) refresh(extend time.Duration) {
	newExpiry := time.Now().Add(extend + time.Duration(l.RemainingTTL())*time.Second)
	l.expiryMu.Lock()
	defer l.expiryMu.Unlock()
	l.expiry = newExpiry
}

// forever sets the expiry of lease to be forever.
// forever 设置 lease 的过期时间为永久。看着是从节点的lease都设置成 永久了
func (l *Lease) forever() {
	l.expiryMu.Lock()
	defer l.expiryMu.Unlock()
	l.expiry = forever
}

// Keys returns all the keys attached to the lease.
// Keys 返回所有附加到lease的keys
func (l *Lease) Keys() []string {
	l.mu.RLock()
	keys := make([]string, 0, len(l.itemSet))
	for k := range l.itemSet {
		keys = append(keys, k.Key)
	}
	l.mu.RUnlock()
	return keys
}

// Remaining returns the remaining time of the lease.
// Remaining 返回 lease 的剩余时间。
func (l *Lease) Remaining() time.Duration {
	l.expiryMu.RLock()
	defer l.expiryMu.RUnlock()
	if l.expiry.IsZero() { //如果到期时间 expiry 是 0 点。那么就没有过期时间。
		return time.Duration(math.MaxInt64)
	}
	return time.Until(l.expiry) //应该是expiry-当前时间。如果>0就是剩余时间嘛，如果<=0就是已经过期了。
	// 这个time.Until(t) 跟 time.Since(t) 正好反过来。 since 是 当前时间-t。 until是t-当前时间。
}

type LeaseItem struct {
	Key string
}

func int64ToBytes(n int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(n))
	return bytes
}

// FakeLessor is a fake implementation of Lessor interface.
// Used for testing only.
type FakeLessor struct{}

func (fl *FakeLessor) SetRangeDeleter(dr RangeDeleter) {}

func (fl *FakeLessor) SetCheckpointer(cp Checkpointer) {}

func (fl *FakeLessor) Grant(id LeaseID, ttl int64) (*Lease, error) { return nil, nil }

func (fl *FakeLessor) Revoke(id LeaseID) error { return nil }

func (fl *FakeLessor) Checkpoint(id LeaseID, remainingTTL int64) error { return nil }

func (fl *FakeLessor) Attach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) GetLease(item LeaseItem) LeaseID            { return 0 }
func (fl *FakeLessor) Detach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) Promote(extend time.Duration) {}

func (fl *FakeLessor) Demote() {}

func (fl *FakeLessor) Renew(id LeaseID) (int64, error) { return 10, nil }

func (fl *FakeLessor) Lookup(id LeaseID) *Lease { return nil }

func (fl *FakeLessor) Leases() []*Lease { return nil }

func (fl *FakeLessor) ExpiredLeasesC() <-chan []*Lease { return nil }

func (fl *FakeLessor) Recover(b backend.Backend, rd RangeDeleter) {}

func (fl *FakeLessor) Stop() {}

type FakeTxnDelete struct {
	backend.BatchTx
}

func (ftd *FakeTxnDelete) DeleteRange(key, end []byte) (n, rev int64) { return 0, 0 }
func (ftd *FakeTxnDelete) End()                                       { ftd.Unlock() }
