// Copyright 2019 The etcd Authors
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

package tracker

// StateType is the state of a tracked follower.
type StateType uint64

const (
	// StateProbe indicates a follower whose last index isn't known. Such a
	// follower is "probed" (i.e. an append sent periodically) to narrow down
	// its last index. In the ideal (and common) case, only one round of probing
	// is necessary as the follower will react with a hint. Followers that are
	// probed over extended periods of time are often offline.
	// StateProbe 代表 最后的index 未知的 follower。对 follower 进行 “探测”（定期发送附件）， 来缩小其最后一个的范围。
	// 在理想（和常见）情况下，只需进行一轮探测，因为追随者会做出提示。 长时间跟踪的follower通常处于离线状态。
	StateProbe StateType = iota
	// StateReplicate is the state steady in which a follower eagerly receives
	// log entries to append to its log.
	//StateReplicate是状态稳定状态，在此状态下，follower 渴望接收日志条目以追加到其日志中。
	StateReplicate
	// StateSnapshot indicates a follower that needs log entries not available
	// from the leader's Raft log. Such a follower needs a full snapshot to
	// return to StateReplicate.
	// StateSnapshot表示关注者需要从领导者的Raft日志中找不到的日志条目。 这样的关注者需要完整的快照才能返回StateReplicate。
	StateSnapshot
)

var prstmap = [...]string{
	"StateProbe",
	"StateReplicate",
	"StateSnapshot",
}

func (st StateType) String() string { return prstmap[uint64(st)] }
