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

package raft

import pb "go.etcd.io/etcd/v3/raft/raftpb"

// ReadState provides state for read only query.
// ReadState为只读查询提供状态。
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// RequestCtx
//调用者的责任是在从ready获取这个状态之前首先调用ReadIndex，而且调用者也有责任区分这个状态是否是它通过RequestCtx请求的，例如给定一个惟一的id作为RequestCtx
type ReadState struct {
	Index      uint64
	RequestCtx []byte
}

type readIndexStatus struct {
	req   pb.Message
	index uint64
	// NB: this never records 'false', but it's more convenient to use this
	// instead of a map[uint64]struct{} due to the API of quorum.VoteResult. If
	// this becomes performance sensitive enough (doubtful), quorum.VoteResult
	// can change to an API that is closer to that of CommittedIndex.
	// 注： 这永远不记录‘false’，但是用它来替代map[uint64]struct{}更方便,因为 quorum.VoteResult这个api
	// 如果这对性能足够敏感，quorum.VoteResult 可以更换为 更接近 CommittedIndex的api
	acks map[uint64]bool
}

type readOnly struct {
	option           ReadOnlyOption
	pendingReadIndex map[string]*readIndexStatus
	readIndexQueue   []string
}

func newReadOnly(option ReadOnlyOption) *readOnly {
	return &readOnly{
		option:           option,
		pendingReadIndex: make(map[string]*readIndexStatus),
	}
}

// addRequest adds a read only request into readonly struct.
// addRequest将只读请求添加到只读结构中。
// `index` is the commit index of the raft state machine when it received
// the read only request.
// `index`是raft状态机收到只读请求时的提交索引。
// `m` is the original read only request message from the local or remote node.
// `m`是来自本地或远程节点的原始只读请求消息。
func (ro *readOnly) addRequest(index uint64, m pb.Message) {
	// 拿Message中的Entries中的第一个Data，然后判断是否已经有了，没有就加上，有就直接return
	s := string(m.Entries[0].Data)
	if _, ok := ro.pendingReadIndex[s]; ok {
		return
	}
	ro.pendingReadIndex[s] = &readIndexStatus{index: index, req: m, acks: make(map[uint64]bool)}
	ro.readIndexQueue = append(ro.readIndexQueue, s)
}

// recvAck notifies the readonly struct that the raft state machine received
// an acknowledgment of the heartbeat that attached with the read only request
// context.
//recvAck通知readonly结构raft状态机接收到了附加到只读请求上下文的heartbeat的确认。
func (ro *readOnly) recvAck(id uint64, context []byte) map[uint64]bool {
	rs, ok := ro.pendingReadIndex[string(context)]
	if !ok {
		return nil
	}

	rs.acks[id] = true
	return rs.acks
}

// advance advances the read only request queue kept by the readonly struct.
// advance推进readonly struct 保留的 只读请求队列（read only request）。
// It dequeues the requests until it finds the read only request that has
// the same context as the given `m`.
// 它从队列中取数据，直到找到read only request和给定的`m`有相同的context
// 找到 和 `m` 一样的那个元素，删除`m`和之前的元素，并返回删除的readIndexStatus
func (ro *readOnly) advance(m pb.Message) []*readIndexStatus {
	var (
		i     int
		found bool
	)

	ctx := string(m.Context)
	rss := []*readIndexStatus{}

	for _, okctx := range ro.readIndexQueue {
		i++
		rs, ok := ro.pendingReadIndex[okctx]
		if !ok {
			panic("cannot find corresponding read state from pending map")
		}
		rss = append(rss, rs)
		if okctx == ctx {
			found = true
			break
		}
	}

	if found {
		//找到了就删除前面的
		ro.readIndexQueue = ro.readIndexQueue[i:]
		for _, rs := range rss {
			delete(ro.pendingReadIndex, string(rs.req.Entries[0].Data))
		}
		return rss
	}

	return nil
}

// lastPendingRequestCtx returns the context of the last pending read only
// request in readonly struct.
// lastPendingRequestCtx 返回在只读struct中的最后一个追加的只读请求的context
func (ro *readOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}
