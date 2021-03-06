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
	"sync"
	"time"

	"go.etcd.io/etcd/v3/etcdserver/cindex"
	"go.etcd.io/etcd/v3/lease"
	"go.etcd.io/etcd/v3/mvcc/backend"
	"go.etcd.io/etcd/v3/mvcc/mvccpb"
	"go.etcd.io/etcd/v3/pkg/traceutil"

	"go.uber.org/zap"
)

// non-const so modifiable by tests 不是常量，所以可以在测试的时候修改
var (
	// chanBufLen is the length of the buffered chan
	// for sending out watched events.
	// chanBufLen是用于发出监视事件的缓冲chan的长度。
	// See https://github.com/etcd-io/etcd/issues/11906 for more detail.
	chanBufLen = 128

	// maxWatchersPerSync is the number of watchers to sync in a single batch
	// maxWatchersPerSync是单个批次中要同步的观察者的数量
	maxWatchersPerSync = 512
)

type watchable interface {
	watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc)
	progress(w *watcher)
	rev() int64
}

type watchableStore struct {
	*store

	// mu protects watcher groups and batches. It should never be locked
	// before locking store.mu to avoid deadlock.
	mu sync.RWMutex

	// victims are watcher batches that were blocked on the watch channel
	// 受害者是在监视频道中被阻止的监视程序批次
	// 1. 当put的时候，如果产生的event不可以直接发送到watch.ch中，那么就放在这里暂存，然后往victimc中发个通知（如果victimc中已经有通知了，就不管了，不会阻塞）
	victims []watcherBatch
	victimc chan struct{} // 缓冲是1的chan

	// contains all unsynced watchers that needs to sync with events that have happened
	// 包换所有未同步的watchers。这些watchers 同步已经发生的是事件。
	// 记录了观察当前revision之前的revision的watcher
	unsynced watcherGroup

	// contains all synced watchers that are in sync with the progress of the store.
	// The key of the map is the key that the watcher watches on.
	// 包含所有的以同步的watchers，这些watchers 已经被store的progress同步过了。
	// mpa的key是 watcher 观察的key
	// 记录了观察当前revision之后的revision的watcher
	synced watcherGroup

	stopc chan struct{}
	wg    sync.WaitGroup
}

// cancelFunc updates unsynced and synced maps when running
// cancel operations.
// cancelFunc 更新同步的和未同步的maps，当运行cancel操作的时候。
type cancelFunc func()

func New(lg *zap.Logger, b backend.Backend, le lease.Lessor, ci cindex.ConsistentIndexer, cfg StoreConfig) ConsistentWatchableKV {
	return newWatchableStore(lg, b, le, ci, cfg)
}

func newWatchableStore(lg *zap.Logger, b backend.Backend, le lease.Lessor, ci cindex.ConsistentIndexer, cfg StoreConfig) *watchableStore {
	if lg == nil {
		lg = zap.NewNop()
	}
	s := &watchableStore{
		store:    NewStore(lg, b, le, ci, cfg),
		victimc:  make(chan struct{}, 1),
		unsynced: newWatcherGroup(),
		synced:   newWatcherGroup(),
		stopc:    make(chan struct{}),
	}
	s.store.ReadView = &readView{s}
	s.store.WriteView = &writeView{s}
	if s.le != nil {
		// use this store as the deleter so revokes trigger watch events
		// 使用此存储作为删除器，因此撤消触发器监视事件
		s.le.SetRangeDeleter(func() lease.TxnDelete { return s.Write(traceutil.TODO()) })
	}
	s.wg.Add(2)
	go s.syncWatchersLoop()
	go s.syncVictimsLoop()
	return s
}

func (s *watchableStore) Close() error {
	close(s.stopc)
	s.wg.Wait()
	return s.store.Close()
}

func (s *watchableStore) NewWatchStream() WatchStream {
	watchStreamGauge.Inc()
	return &watchStream{
		watchable: s, // 把自己传进去了
		ch:        make(chan WatchResponse, chanBufLen),
		cancels:   make(map[WatchID]cancelFunc),
		watchers:  make(map[WatchID]*watcher),
	}
}

func (s *watchableStore) watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc) {
	wa := &watcher{ // 创建了一个新的watcher
		key:    key,
		end:    end,
		minRev: startRev,
		id:     id,
		ch:     ch,
		fcs:    fcs,
	}

	s.mu.Lock()
	s.revMu.RLock()
	synced := startRev > s.store.currentRev || startRev == 0
	if synced {
		wa.minRev = s.store.currentRev + 1
		if startRev > wa.minRev {
			wa.minRev = startRev
		}
	}
	if synced {
		s.synced.add(wa)
	} else {
		slowWatcherGauge.Inc()
		s.unsynced.add(wa)
	}
	s.revMu.RUnlock()
	s.mu.Unlock()

	watcherGauge.Inc()

	return wa, func() { s.cancelWatcher(wa) }
}

// cancelWatcher removes references of the watcher from the watchableStore
// cancelWatcher从watchableStore中删除监视者的引用
func (s *watchableStore) cancelWatcher(wa *watcher) {
	for {
		s.mu.Lock()
		if s.unsynced.delete(wa) {
			slowWatcherGauge.Dec()
			watcherGauge.Dec()
			break
		} else if s.synced.delete(wa) {
			watcherGauge.Dec()
			break
		} else if wa.compacted {
			watcherGauge.Dec()
			break
		} else if wa.ch == nil {
			// already canceled (e.g., cancel/close race)
			break
		}

		if !wa.victim {
			s.mu.Unlock()
			panic("watcher not victim but not in watch groups")
		}

		var victimBatch watcherBatch
		for _, wb := range s.victims {
			if wb[wa] != nil {
				victimBatch = wb
				break
			}
		}
		if victimBatch != nil {
			slowWatcherGauge.Dec()
			watcherGauge.Dec()
			delete(victimBatch, wa)
			break
		}

		// victim being processed so not accessible; retry
		s.mu.Unlock()
		time.Sleep(time.Millisecond)
	}

	wa.ch = nil
	s.mu.Unlock()
}

func (s *watchableStore) Restore(b backend.Backend) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.store.Restore(b)
	if err != nil {
		return err
	}

	for wa := range s.synced.watchers {
		wa.restore = true
		s.unsynced.add(wa)
	}
	s.synced = newWatcherGroup()
	return nil
}

// syncWatchersLoop syncs the watcher in the unsynced map every 100ms.
// syncWatchersLoop 在 unsynced map 中每 100ms 同步一次 watcher
func (s *watchableStore) syncWatchersLoop() {
	defer s.wg.Done()

	for {
		s.mu.RLock()
		st := time.Now()
		lastUnsyncedWatchers := s.unsynced.size()
		s.mu.RUnlock()

		unsyncedWatchers := 0
		if lastUnsyncedWatchers > 0 {
			unsyncedWatchers = s.syncWatchers()
		}
		syncDuration := time.Since(st)

		waitDuration := 100 * time.Millisecond
		// more work pending?
		if unsyncedWatchers != 0 && lastUnsyncedWatchers > unsyncedWatchers {
			// be fair to other store operations by yielding time taken
			// 通过节省时间来公平对待其他store 运营
			waitDuration = syncDuration
		}

		select {
		case <-time.After(waitDuration):
		case <-s.stopc:
			return
		}
	}
}

// syncVictimsLoop tries to write precomputed watcher responses to
// watchers that had a blocked watcher channel
// syncVictimsLoop尝试将预先计算的观察者响应写入观察者通道被阻止的观察者
func (s *watchableStore) syncVictimsLoop() {
	defer s.wg.Done()

	for {
		for s.moveVictims() != 0 {
			// try to update all victim watchers
		}
		s.mu.RLock()
		isEmpty := len(s.victims) == 0
		s.mu.RUnlock()

		var tickc <-chan time.Time
		if !isEmpty { //如果不是空的，给个定时器，如果是空的，不给定时器。这样如果victimc不来通知，会一直阻塞。
			tickc = time.After(10 * time.Millisecond)
		}

		select {
		case <-tickc:
		case <-s.victimc:
		case <-s.stopc:
			return
		}
	}
}

// moveVictims tries to update watches with already pending event data
// 做了3件事 处理 s.victims 中的watcher和events
// 1. 尝试重新发送response，如果成功返回值 moved++
// 2. 如果重新发送response成功了，将watch放回到unsync/sync group中
// 3. 如果没成功，重新放回到 s.victims 中
func (s *watchableStore) moveVictims() (moved int) {
	s.mu.Lock()
	victims := s.victims //取出来并置空
	s.victims = nil
	s.mu.Unlock()

	var newVictim watcherBatch
	for _, wb := range victims {
		// try to send responses again
		// 尝试重新发送
		for w, eb := range wb {
			// watcher has observed the store up to, but not including, w.minRev
			rev := w.minRev - 1
			if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: rev}) {
				pendingEventsGauge.Add(float64(len(eb.evs)))
			} else {
				if newVictim == nil {
					newVictim = make(watcherBatch)
				}
				newVictim[w] = eb
				continue
			}
			moved++
		}

		// assign completed victim watchers to unsync/sync
		// 将重新send完成的victim watchers重新放进 unsync/sync group中
		s.mu.Lock()
		s.store.revMu.RLock()
		curRev := s.store.currentRev
		for w, eb := range wb {
			if newVictim != nil && newVictim[w] != nil {
				// couldn't send watch response; stays victim
				// 没有重新send成功，保留到victim中
				continue
			}
			w.victim = false
			if eb.moreRev != 0 {
				w.minRev = eb.moreRev
			}
			if w.minRev <= curRev {
				s.unsynced.add(w)
			} else {
				slowWatcherGauge.Dec()
				s.synced.add(w)
			}
		}
		s.store.revMu.RUnlock()
		s.mu.Unlock()
	}

	//没有重新发送成功的，重新填充到 victims 中
	if len(newVictim) > 0 {
		s.mu.Lock()
		s.victims = append(s.victims, newVictim)
		s.mu.Unlock()
	}

	return moved
}

// syncWatchers syncs unsynced watchers by:
//	1. choose a set of watchers from the unsynced watcher group
//	2. iterate over the set to get the minimum revision and remove compacted watchers
//	3. use minimum revision to get all key-value pairs and send those events to watchers
//	4. remove synced watchers in set from unsynced group and move to synced group
// syncWatchers 同步 unsynced watchers，有如下几个步骤：
// 1. 从unsynced watcher group 中 选择一组 watchers
// 2. 遍历该 set ，获取到最小的 revision 并且 删除掉 已经压缩的 watchers
// 3. 用 最小小的 revision 获取到 所有的 key-value 对。然后发送这些事件给 watchers
// 4. 将 synced watchers 在unsynced group set中删除，然后移动到 synced group 中。
func (s *watchableStore) syncWatchers() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unsynced.size() == 0 {
		return 0
	}

	s.store.revMu.RLock()
	defer s.store.revMu.RUnlock()

	// in order to find key-value pairs from unsynced watchers, we need to
	// find min revision index, and these revisions can be used to
	// query the backend store of key-value pairs
	curRev := s.store.currentRev
	compactionRev := s.store.compactMainRev

	wg, minRev := s.unsynced.choose(maxWatchersPerSync, curRev, compactionRev)
	minBytes, maxBytes := newRevBytes(), newRevBytes()
	revToBytes(revision{main: minRev}, minBytes)
	revToBytes(revision{main: curRev + 1}, maxBytes)

	// UnsafeRange returns keys and values. And in boltdb, keys are revisions.
	// values are actual key-value pairs in backend.
	tx := s.store.b.ReadTx()
	tx.RLock()
	revs, vs := tx.UnsafeRange(keyBucketName, minBytes, maxBytes, 0)
	tx.RUnlock()
	var evs []mvccpb.Event
	evs = kvsToEvents(s.store.lg, wg, revs, vs)

	var victims watcherBatch
	wb := newWatcherBatch(wg, evs)
	for w := range wg.watchers {
		w.minRev = curRev + 1

		eb, ok := wb[w]
		if !ok {
			// bring un-notified watcher to synced
			s.synced.add(w)
			s.unsynced.delete(w)
			continue
		}

		if eb.moreRev != 0 {
			w.minRev = eb.moreRev
		}

		if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: curRev}) {
			pendingEventsGauge.Add(float64(len(eb.evs)))
		} else {
			if victims == nil {
				victims = make(watcherBatch)
			}
			w.victim = true
		}

		if w.victim {
			victims[w] = eb
		} else {
			if eb.moreRev != 0 {
				// stay unsynced; more to read
				continue
			}
			s.synced.add(w)
		}
		s.unsynced.delete(w)
	}
	s.addVictim(victims)

	vsz := 0
	for _, v := range s.victims {
		vsz += len(v)
	}
	slowWatcherGauge.Set(float64(s.unsynced.size() + vsz))

	return s.unsynced.size()
}

// kvsToEvents gets all events for the watchers from all key-value pairs
func kvsToEvents(lg *zap.Logger, wg *watcherGroup, revs, vals [][]byte) (evs []mvccpb.Event) {
	for i, v := range vals {
		var kv mvccpb.KeyValue
		if err := kv.Unmarshal(v); err != nil {
			lg.Panic("failed to unmarshal mvccpb.KeyValue", zap.Error(err))
		}

		if !wg.contains(string(kv.Key)) {
			continue
		}

		ty := mvccpb.PUT
		if isTombstone(revs[i]) {
			ty = mvccpb.DELETE
			// patch in mod revision so watchers won't skip
			kv.ModRevision = bytesToRev(revs[i]).main
		}
		evs = append(evs, mvccpb.Event{Kv: &kv, Type: ty})
	}
	return evs
}

// notify notifies the fact that given event at the given rev just happened to
// watchers that watch on the key of the event.
// notify通知一个事实，即在给定版本上的给定事件只是发生在监视事件键的观察者身上。
// 参数 rev 是当前事务的 revision
func (s *watchableStore) notify(rev int64, evs []mvccpb.Event) {
	var victim watcherBatch
	for w, eb := range newWatcherBatch(&s.synced, evs) { //新的事件的watcher，都是在s.synced中拿的，因为unsynced中还有历史版本没有watch完
		if eb.revs != 1 { //当前通知的一定是一个事务内的，所以 revs 代表不同的 ModRevision 的计数，应该是1
			s.store.lg.Panic(
				"unexpected multiple revisions in watch notification",
				zap.Int("number-of-revisions", eb.revs),
			)
		}
		if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: rev}) {
			pendingEventsGauge.Add(float64(len(eb.evs)))
		} else { //这个else就是说，watcher.ch 已经满了
			// move slow watcher to victims [将慢速观察者移交给受害者]
			w.minRev = rev + 1
			if victim == nil {
				victim = make(watcherBatch)
			}
			w.victim = true
			victim[w] = eb
			s.synced.delete(w)
			slowWatcherGauge.Inc()
		}
	}
	s.addVictim(victim)
}

func (s *watchableStore) addVictim(victim watcherBatch) {
	if victim == nil {
		return
	}
	s.victims = append(s.victims, victim)
	select {
	case s.victimc <- struct{}{}:
	default:
	}
}

func (s *watchableStore) rev() int64 { return s.store.Rev() }

func (s *watchableStore) progress(w *watcher) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.synced.watchers[w]; ok {
		w.send(WatchResponse{WatchID: w.id, Revision: s.rev()})
		// If the ch is full, this watcher is receiving events.
		// We do not need to send progress at all.
	}
}

type watcher struct {
	// the watcher key
	// 监听的 key
	key []byte
	// end indicates the end of the range to watch.
	// If end is set, the watcher is on a range.
	// watcher 可以监听一个key，也可以监听一个range，如果end是nil，那么该watcher监听的就是个key，否则就是个range
	end []byte

	// victim is set when ch is blocked and undergoing victim processing
	// victim 在当 ch被阻塞或者victim进行的时候 被设置为true
	victim bool

	// compacted is set when the watcher is removed because of compaction
	// compacted 当该watcher因为压缩被删除的时候，该watcher被设置为true
	compacted bool

	// restore is true when the watcher is being restored from leader snapshot
	// which means that this watcher has just been moved from "synced" to "unsynced"
	// watcher group, possibly with a future revision when it was first added
	// to the synced watcher
	//当从领导者快照还原观察者时，restore为true，这意味着该观察者刚刚从“已同步”观察者组移至“未同步”观察者组，
	//并且可能在首次添加到已同步观察者时具有将来的修订版本
	// "unsynced" watcher revision must always be <= current revision,
	// except when the watcher were to be moved from "synced" watcher group
	//“不同步的”观察者修订版必须始终为<=当前修订版，除非要将观察者从“已同步”的观察者组中移出
	restore bool

	// minRev is the minimum revision update the watcher will accept
	// minRev是观察者将接受的最低修订版本
	minRev int64
	id     WatchID

	fcs []FilterFunc
	// a chan to send out the watch response.
	// The chan might be shared with other watchers.
	// chan发出监视响应。
	// chan可能与其他观察者共享。
	ch chan<- WatchResponse
}

func (w *watcher) send(wr WatchResponse) bool {
	progressEvent := len(wr.Events) == 0 //如果没有 events，progressEvent 就是 true

	// 这个操作就是 用w.fcs过滤了一下wr.Events。
	if len(w.fcs) != 0 {
		ne := make([]mvccpb.Event, 0, len(wr.Events))
		for i := range wr.Events {
			filtered := false
			for _, filter := range w.fcs {
				if filter(wr.Events[i]) {
					filtered = true
					break
				}
			}
			if !filtered {
				ne = append(ne, wr.Events[i])
			}
		}
		wr.Events = ne
	}

	// if all events are filtered out, we should send nothing.
	// 如果所有的events已经被过滤掉了，我们应该不发送了就
	// 这里就是 过滤之后 events 才是0，之前不是，那么就返回true
	// progressEvent 就是代表，参数给定的events是否是空
	// 所以这个判断就是，如果给定参数的events不空，并且过滤之后空了，那就直接返回true
	if !progressEvent && len(wr.Events) == 0 {
		return true
	}
	// 到这里，如果参数的events直接给的空的也会到这里。因为progressEvent会使true
	select {
	case w.ch <- wr: // 扔到了w.ch中。  ch 生产。
		return true
	default:
		return false
	}
}
