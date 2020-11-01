// Copyright 2016 The etcd Authors
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
	"fmt"
	"math"

	"go.etcd.io/etcd/v3/mvcc/mvccpb"
	"go.etcd.io/etcd/v3/pkg/adt"
)

var (
	// watchBatchMaxRevs is the maximum distinct revisions that
	// may be sent to an unsynced watcher at a time. Declared as
	// var instead of const for testing purposes.
	// watchBatchMaxRevs是一次可以发送给 unsynced watcher 的最大不同修订版本。 为了测试目的，声明为var而不是const。
	watchBatchMaxRevs = 1000
)

type eventBatch struct {
	// evs is a batch of revision-ordered events
	// evs是一批按修订版本排序的事件
	evs []mvccpb.Event
	// revs is the minimum unique revisions observed for this batch
	// revs 是一个计数。该event的Kv的ModRevision有多少个不同的，该计数就是几。
	revs int
	// moreRev is first revision with more events following this batch
	// moreRev 是该批次多出的events的第一个，比如watchBatchMaxRevs限定了以批次最对1000个，那么moreRev就是第1001个ModRevision
	moreRev int64
}

func (eb *eventBatch) add(ev mvccpb.Event) {
	if eb.revs > watchBatchMaxRevs {
		// maxed out batch size
		return
	}

	if len(eb.evs) == 0 {
		// base case
		eb.revs = 1
		eb.evs = append(eb.evs, ev)
		return
	}

	// revision accounting
	ebRev := eb.evs[len(eb.evs)-1].Kv.ModRevision
	evRev := ev.Kv.ModRevision
	if evRev > ebRev {
		eb.revs++
		if eb.revs > watchBatchMaxRevs {
			eb.moreRev = evRev
			return
		}
	}

	eb.evs = append(eb.evs, ev)
}

type watcherBatch map[*watcher]*eventBatch

func (wb watcherBatch) add(w *watcher, ev mvccpb.Event) {
	eb := wb[w]
	if eb == nil {
		eb = &eventBatch{}
		wb[w] = eb
	}
	eb.add(ev)
}

// newWatcherBatch maps watchers to their matched events. It enables quick
// events look up by watcher.
// newWatcherBatch将观察者映射到其匹配的事件。 它使观察者可以快速查找事件。
func newWatcherBatch(wg *watcherGroup, evs []mvccpb.Event) watcherBatch {
	if len(wg.watchers) == 0 {
		return nil
	}

	wb := make(watcherBatch)
	for _, ev := range evs {
		//遍历所有监听该key的watcher
		for w := range wg.watcherSetByKey(string(ev.Kv.Key)) {
			if ev.Kv.ModRevision >= w.minRev { // todo 这个判断啥意思？
				// don't double notify
				wb.add(w, ev)
			}
		}
	}
	return wb
}

// 就是一个watcherSet的集合。
type watcherSet map[*watcher]struct{}

func (w watcherSet) add(wa *watcher) {
	if _, ok := w[wa]; ok {
		panic("add watcher twice!")
	}
	w[wa] = struct{}{}
}

// 将参数ws的数据watcher复制到 w上
func (w watcherSet) union(ws watcherSet) {
	for wa := range ws {
		w.add(wa)
	}
}

func (w watcherSet) delete(wa *watcher) {
	if _, ok := w[wa]; !ok {
		panic("removing missing watcher!")
	}
	delete(w, wa)
}

// key -> watcherSet 的 map，value是watcherSet 是因为一个key可能被多个watcher监听。
type watcherSetByKey map[string]watcherSet

func (w watcherSetByKey) add(wa *watcher) {
	set := w[string(wa.key)]
	if set == nil {
		set = make(watcherSet)
		w[string(wa.key)] = set
	}
	set.add(wa)
}

func (w watcherSetByKey) delete(wa *watcher) bool {
	k := string(wa.key)
	if v, ok := w[k]; ok {
		if _, ok := v[wa]; ok {
			delete(v, wa)
			if len(v) == 0 {
				// remove the set; nothing left
				delete(w, k)
			}
			return true
		}
	}
	return false
}

// watcherGroup is a collection of watchers organized by their ranges
// watcherGroup是按观察者范围组织的观察者集合
type watcherGroup struct {
	// keyWatchers has the watchers that watch on a single key
	// keyWatchers 拥有 watchers，这些watchers观察单个的key
	// 就是一个 key -> watcherSet的集合
	// 比如有一个key = key1  被 watcher1， watcher2 监听
	// 那么这里就会有个keyWatchers : key1 => {watcher1:struct{}, watcher2:struct{}}
	keyWatchers watcherSetByKey
	// ranges has the watchers that watch a range; it is sorted by interval
	// ranges 拥有 watchers，这些watchers 观察一个 key范围；按照时间间隔排序。
	ranges adt.IntervalTree
	// watchers is the set of all watchers
	// watchers 是所有watchers的集合
	// 比如有一个key = key1  被 watcher1， watcher2 监听, key = key2 被watcher3监听
	// 会有 watchers：{watcher1:struct{}, watcher2:struct{}, watcher3:struct{}}
	watchers watcherSet
}

func newWatcherGroup() watcherGroup {
	return watcherGroup{
		keyWatchers: make(watcherSetByKey),
		ranges:      adt.NewIntervalTree(),
		watchers:    make(watcherSet),
	}
}

// add puts a watcher in the group.
// 在group中增加一个 watcher
func (wg *watcherGroup) add(wa *watcher) {
	wg.watchers.add(wa) // 先加到 所有的watcher集合 wg.watchers 中
	if wa.end == nil {
		// 如果 end 是nil，代表是个监听单key的watcher，直接放进 keyWatchers 中就行。
		wg.keyWatchers.add(wa)
		return
	}

	// interval already registered? 间隔已经注册？已经被注册就add一个，没有就新建再注册
	ivl := adt.NewStringAffineInterval(string(wa.key), string(wa.end))
	if iv := wg.ranges.Find(ivl); iv != nil {
		iv.Val.(watcherSet).add(wa)
		return
	}

	// not registered, put in interval tree
	ws := make(watcherSet)
	ws.add(wa)
	wg.ranges.Insert(ivl, ws)
}

// contains is whether the given key has a watcher in the group.
// 返回是否给定的key有watcher在group中。
func (wg *watcherGroup) contains(key string) bool {
	_, ok := wg.keyWatchers[key]
	return ok || wg.ranges.Intersects(adt.NewStringAffinePoint(key))
}

// size gives the number of unique watchers in the group.
// size 返回group中所有的唯一的watchers的数量
func (wg *watcherGroup) size() int { return len(wg.watchers) }

// delete removes a watcher from the group.
func (wg *watcherGroup) delete(wa *watcher) bool {
	if _, ok := wg.watchers[wa]; !ok {
		return false //没有该watcher就直接返回false
	}
	wg.watchers.delete(wa) // 有 就删除
	if wa.end == nil {
		wg.keyWatchers.delete(wa) //单key删除watcher
		return true
	}

	ivl := adt.NewStringAffineInterval(string(wa.key), string(wa.end))
	iv := wg.ranges.Find(ivl)
	if iv == nil {
		return false
	}

	ws := iv.Val.(watcherSet)
	delete(ws, wa) // 如果是range watcher ，也删除。
	if len(ws) == 0 {
		// remove interval missing watchers
		if ok := wg.ranges.Delete(ivl); !ok {
			panic("could not remove watcher from interval tree")
		}
	}

	return true
}

// choose selects watchers from the watcher group to update
// 返回一个新的watcherGroup，新的group的最大的watchers是给定的参数 maxWatchers 。
func (wg *watcherGroup) choose(maxWatchers int, curRev, compactRev int64) (*watcherGroup, int64) {
	if len(wg.watchers) < maxWatchers { // 如果最多也不超过 maxWatchers 直接返回。
		return wg, wg.chooseAll(curRev, compactRev)
	}
	//新建一个group，并取最多 maxWatchers 个watcher。
	ret := newWatcherGroup()
	for w := range wg.watchers {
		if maxWatchers <= 0 {
			break
		}
		maxWatchers--
		ret.add(w)
	}
	return &ret, ret.chooseAll(curRev, compactRev)
}

// 返回值就是所以的watcher中，minRev最小的那个吧。
func (wg *watcherGroup) chooseAll(curRev, compactRev int64) int64 {
	minRev := int64(math.MaxInt64)
	for w := range wg.watchers {
		if w.minRev > curRev { // watcher监听的所有的key都还没有被存到db中。
			// after network partition, possibly choosing future revision watcher from restore operation
			// with watch key "proxy-namespace__lostleader" and revision "math.MaxInt64 - 2"
			// do not panic when such watcher had been moved from "synced" watcher during restore operation
			if !w.restore {
				panic(fmt.Errorf("watcher minimum revision %d should not exceed current revision %d", w.minRev, curRev))
			}

			// mark 'restore' done, since it's chosen  标记“恢复”完成，因为已选择
			w.restore = false
		}
		if w.minRev < compactRev { //监听的key的rev比压缩的rev还小。说明该删除了吧。
			select {
			case w.ch <- WatchResponse{WatchID: w.id, CompactRevision: compactRev}: //告知删除
				w.compacted = true
				wg.delete(w)
			default:
				// retry next time
			}
			continue
		}
		if minRev > w.minRev {
			minRev = w.minRev
		}
	}
	return minRev
}

// watcherSetByKey gets the set of watchers that receive events on the given key.
// watcherSetByKey 获取watchers的集合。该watchers接收给定的key的事件。
func (wg *watcherGroup) watcherSetByKey(key string) watcherSet {
	wkeys := wg.keyWatchers[key]                             //取出监听该key的所有unique key watcher
	wranges := wg.ranges.Stab(adt.NewStringAffinePoint(key)) //取出监听该key的所有range key watcher

	// zero-copy cases 0拷贝？ 就是不重新copy了呗，直接把值给返回了。
	switch {
	case len(wranges) == 0:
		// no need to merge ranges or copy; reuse single-key set
		return wkeys
	case len(wranges) == 0 && len(wkeys) == 0:
		return nil
	case len(wranges) == 1 && len(wkeys) == 0:
		return wranges[0].Val.(watcherSet)
	}

	// copy case
	ret := make(watcherSet)
	ret.union(wg.keyWatchers[key])
	for _, item := range wranges {
		ret.union(item.Val.(watcherSet))
	}
	return ret
}
