// Copyright 2018 The etcd Authors
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

import "container/heap"

// LeaseWithTime contains lease object with a time.
// LeaseWithTime 包含一个 lease 对象，该对象有一个时间 time
// For the lessor's lease heap, time identifies the lease expiration time.
// 对于出租人的租赁堆，时间标识租赁到期时间。
// For the lessor's lease checkpoint heap, the time identifies the next lease checkpoint time.
// 对于出租人的租赁检查点堆，该时间标识下一个租赁检查点时间。
type LeaseWithTime struct {
	id LeaseID
	// Unix nanos timestamp. Unix nanos时间戳。
	time  int64
	index int // 队列中第几个放进去的。比如队列中有 3个，那么index分别是 0 1 2
}

type LeaseQueue []*LeaseWithTime

func (pq LeaseQueue) Len() int { return len(pq) }

// 排序是以time排的序
func (pq LeaseQueue) Less(i, j int) bool {
	return pq[i].time < pq[j].time
}

func (pq LeaseQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *LeaseQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*LeaseWithTime)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *LeaseQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// LeaseExpiredNotifier is a queue used to notify lessor to revoke expired lease.
// LeaseExpiredNotifier 是一个 queue 被用来 通知 lessor 来撤销 expired lease
// Only save one item for a lease, `Register` will update time of the corresponding lease.
// 只有保存一份租约，“注册”将更新相应租约的时间。
type LeaseExpiredNotifier struct {
	m     map[LeaseID]*LeaseWithTime
	queue LeaseQueue
}

func newLeaseExpiredNotifier() *LeaseExpiredNotifier {
	return &LeaseExpiredNotifier{
		m:     make(map[LeaseID]*LeaseWithTime),
		queue: make(LeaseQueue, 0),
	}
}

func (mq *LeaseExpiredNotifier) Init() {
	heap.Init(&mq.queue) //用堆做啥？核反应吗？
	mq.m = make(map[LeaseID]*LeaseWithTime)
	for _, item := range mq.queue {
		mq.m[item.id] = item
	}
}

func (mq *LeaseExpiredNotifier) RegisterOrUpdate(item *LeaseWithTime) {
	if old, ok := mq.m[item.id]; ok {
		//如果有就更新时间
		old.time = item.time
		heap.Fix(&mq.queue, old.index)
	} else {
		//如果没有就push一个，并保存起来
		heap.Push(&mq.queue, item)
		mq.m[item.id] = item
	}
}

func (mq *LeaseExpiredNotifier) Unregister() *LeaseWithTime {
	// 删除最后一个 LeaseWithTime
	item := heap.Pop(&mq.queue).(*LeaseWithTime)
	delete(mq.m, item.id)
	return item
}

// 取第一个 LeaseWithTime
func (mq *LeaseExpiredNotifier) Poll() *LeaseWithTime {
	if mq.Len() == 0 {
		return nil
	}
	return mq.queue[0]
}

func (mq *LeaseExpiredNotifier) Len() int {
	return len(mq.m)
}
