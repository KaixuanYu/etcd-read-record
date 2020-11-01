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

package mvcc

import (
	"go.etcd.io/etcd/v3/mvcc/mvccpb"
	"go.etcd.io/etcd/v3/pkg/traceutil"
)

func (tw *watchableStoreTxnWrite) End() {
	changes := tw.Changes() //取出 该次 batch（批量事务）过程中 put和delete的 KeyValue
	if len(changes) == 0 {
		tw.TxnWrite.End()
		return
	}

	rev := tw.Rev() + 1
	//开始封装event
	evs := make([]mvccpb.Event, len(changes))
	for i, change := range changes {
		evs[i].Kv = &changes[i]
		if change.CreateRevision == 0 {
			evs[i].Type = mvccpb.DELETE
			evs[i].Kv.ModRevision = rev
		} else {
			evs[i].Type = mvccpb.PUT
		}
	}

	// end write txn under watchable store lock so the updates are visible
	// when asynchronous event posting checks the current store revision
	tw.s.mu.Lock()
	tw.s.notify(rev, evs)
	tw.TxnWrite.End()
	tw.s.mu.Unlock()
}

// 整个 TxnWrite ，就是在原有的 txnWrite 上重写了 End() 方法。用来当key发生put和delete操作的时候通知给watcher
type watchableStoreTxnWrite struct {
	TxnWrite
	s *watchableStore
}

// Write 这里重写，就是为了封装 watcher 的
func (s *watchableStore) Write(trace *traceutil.Trace) TxnWrite {
	return &watchableStoreTxnWrite{s.store.Write(trace), s}
}
