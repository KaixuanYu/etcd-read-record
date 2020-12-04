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

package v2store

import (
	"container/list"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"go.etcd.io/etcd/v3/etcdserver/api/v2error"
)

// A watcherHub contains all subscribed watchers
// watchers is a map with watched path as key and watcher as value
// EventHistory keeps the old events for watcherHub. It is used to help
// watcher to get a continuous event history. Or a watcher might miss the
// event happens between the end of the first watch command and the start
// of the second command.
// 一个 watcherHub 包含所有的 被订阅的 watchers
// watchers 是一个 map， key是path，value是watcher
// EventHistory 给 watcherHub 保存了旧的 events。它帮助watcher获取连续的event历史
// 否则，观察者可能会错过在第一个监视命令的结束与第二个命令的开始之间发生的事件
type watcherHub struct {
	// count must be the first element to keep 64-bit alignment for atomic
	// access
	// count 字段必须是第一个元素，用来保持 64位对齐，为了原子访问
	count int64 // current number of watchers.

	mutex        sync.Mutex
	watchers     map[string]*list.List
	EventHistory *EventHistory
}

// newWatchHub creates a watcherHub. The capacity determines how many events we will
// keep in the eventHistory.
// Typically, we only need to keep a small size of history[smaller than 20K].
// Ideally, it should smaller than 20K/s[max throughput] * 2 * 50ms[RTT] = 2000
// newWatchHub 创建一个 watcherHub. capacity 参数决定 eventHistory 可以保存多少 events。
// 通常，我们只需要存很小的size的history （小于20k）
// 理想情况下，它应该小于20K/s [最大吞吐量] * 2 * 50ms [RTT] = 2000
func newWatchHub(capacity int) *watcherHub {
	return &watcherHub{
		watchers:     make(map[string]*list.List),
		EventHistory: newEventHistory(capacity),
	}
}

// Watch function returns a Watcher.
// If recursive is true, the first change after index under key will be sent to the event channel of the watcher.
// If recursive is false, the first change after index at key will be sent to the event channel of the watcher.
// If index is zero, watch will start from the current index + 1.
// Watch 函数返回一个 Watcher
// 如果 recursive （递归）是true，则键下索引之后的第一个更改将发送到观察者的事件通道。
// 如果 recursive 为false，则键索引处的第一个更改将被发送到观察者的事件通道。(与上面就是under key和at key的区别)
// 如果 index = zero， watch 将从 当前的 index+1 索引开始
func (wh *watcherHub) watch(key string, recursive, stream bool, index, storeIndex uint64) (Watcher, *v2error.Error) {
	reportWatchRequest()
	event, err := wh.EventHistory.scan(key, recursive, index)

	if err != nil {
		err.Index = storeIndex
		return nil, err
	}

	w := &watcher{
		eventChan:  make(chan *Event, 100), // use a buffered channel
		recursive:  recursive,
		stream:     stream,
		sinceIndex: index,
		startIndex: storeIndex,
		hub:        wh,
	}

	wh.mutex.Lock()
	defer wh.mutex.Unlock()
	// If the event exists in the known history, append the EtcdIndex and return immediately
	if event != nil {
		ne := event.Clone()
		ne.EtcdIndex = storeIndex
		w.eventChan <- ne
		return w, nil
	}

	l, ok := wh.watchers[key]

	var elem *list.Element

	if ok { // add the new watcher to the back of the list
		elem = l.PushBack(w)
	} else { // create a new list and add the new watcher
		l = list.New()
		elem = l.PushBack(w)
		wh.watchers[key] = l
	}

	w.remove = func() {
		if w.removed { // avoid removing it twice
			return
		}
		w.removed = true
		l.Remove(elem)
		atomic.AddInt64(&wh.count, -1)
		reportWatcherRemoved()
		if l.Len() == 0 {
			delete(wh.watchers, key)
		}
	}

	atomic.AddInt64(&wh.count, 1)
	reportWatcherAdded()

	return w, nil
}

// 新增一个Event
func (wh *watcherHub) add(e *Event) {
	wh.EventHistory.addEvent(e)
}

// notify function accepts an event and notify to the watchers.
func (wh *watcherHub) notify(e *Event) {
	e = wh.EventHistory.addEvent(e) // add event into the eventHistory

	segments := strings.Split(e.Node.Key, "/")

	currPath := "/"

	// walk through all the segments of the path and notify the watchers
	// if the path is "/foo/bar", it will notify watchers with path "/",
	// "/foo" and "/foo/bar"

	for _, segment := range segments {
		currPath = path.Join(currPath, segment)
		// notify the watchers who interests in the changes of current path
		wh.notifyWatchers(e, currPath, false)
	}
}

func (wh *watcherHub) notifyWatchers(e *Event, nodePath string, deleted bool) {
	wh.mutex.Lock()
	defer wh.mutex.Unlock()

	l, ok := wh.watchers[nodePath]
	if ok {
		curr := l.Front()

		for curr != nil {
			next := curr.Next() // save reference to the next one in the list

			w, _ := curr.Value.(*watcher)

			originalPath := e.Node.Key == nodePath
			if (originalPath || !isHidden(nodePath, e.Node.Key)) && w.notify(e, originalPath, deleted) {
				if !w.stream { // do not remove the stream watcher
					// if we successfully notify a watcher
					// we need to remove the watcher from the list
					// and decrease the counter
					w.removed = true
					l.Remove(curr)
					atomic.AddInt64(&wh.count, -1)
					reportWatcherRemoved()
				}
			}

			curr = next // update current to the next element in the list
		}

		if l.Len() == 0 {
			// if we have notified all watcher in the list
			// we can delete the list
			delete(wh.watchers, nodePath)
		}
	}
}

// clone function clones the watcherHub and return the cloned one.
// only clone the static content. do not clone the current watchers.
func (wh *watcherHub) clone() *watcherHub {
	clonedHistory := wh.EventHistory.clone()

	return &watcherHub{
		EventHistory: clonedHistory,
	}
}

// isHidden checks to see if key path is considered hidden to watch path i.e. the
// last element is hidden or it's within a hidden directory
func isHidden(watchPath, keyPath string) bool {
	// When deleting a directory, watchPath might be deeper than the actual keyPath
	// For example, when deleting /foo we also need to notify watchers on /foo/bar.
	if len(watchPath) > len(keyPath) {
		return false
	}
	// if watch path is just a "/", after path will start without "/"
	// add a "/" to deal with the special case when watchPath is "/"
	afterPath := path.Clean("/" + keyPath[len(watchPath):])
	return strings.Contains(afterPath, "/_")
}
