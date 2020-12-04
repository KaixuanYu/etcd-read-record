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

// 一个 Event 的 队列
type eventQueue struct {
	Events   []*Event
	Size     int
	Front    int
	Back     int
	Capacity int
}

// 插队
func (eq *eventQueue) insert(e *Event) {
	eq.Events[eq.Back] = e
	eq.Back = (eq.Back + 1) % eq.Capacity

	if eq.Size == eq.Capacity { //dequeue 如果队列满了的话，就将队头的event出列，front前移。
		eq.Front = (eq.Front + 1) % eq.Capacity
	} else {
		eq.Size++
	}
}
