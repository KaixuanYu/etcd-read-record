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

import (
	"fmt"
	"sort"
	"strings"
)

// Progress represents a follower’s progress in the view of the leader. Leader
// maintains progresses of all followers, and sends entries to the follower
// based on its progress.
// Progress 代表在leader中的follower的进度。
// leader 有所有的followers的进度，并且根据此进度发送entries给follower
//
// NB(tbg): Progress is basically a state machine whose transitions are mostly
// strewn around `*raft.raft`. Additionally, some fields are only used when in a
// certain State. All of this isn't ideal.
// NB（tbg）：进度基本上是一种状态机，其过渡大部分围绕着** raft.raft`。 此外，某些字段仅在处于特定状态时使用。 所有这些都不理想。
type Progress struct {
	// Leader与Follower之间的状态同步是异步的，Leader将日志发给Follower，Follower再回复接收
	// 到了哪些日志。出于效率考虑，Leader不会每条日志以类似同步调用的方式发送给Follower，而是
	// 只要Leader有新的日志就发送，Next就是用来记录下一次发送日志起始索引。换句话说就是发送给Peer
	// 的最大日志索引是Next-1，而Match的就是经过Follower确认接收的最大日志索引，Next-Match-1
	// 就是还在飞行中或者还在路上的日志数量（Inflights）。Inflights还是比较形象的，下面会有详细
	// 说明。 (0, Next)的日志已经发送给节点了，(0,Match]是节点的已经接收到的日志。(match, next)就是在飞行中的。哈哈哈
	Match, Next uint64
	// State defines how the leader should interact with the follower.
	// State 定义领导者应如何与跟随者互动。
	//
	// When in StateProbe, leader sends at most one replication message
	// per heartbeat interval. It also probes actual progress of the follower.
	// 在StateProbe中时，领导者每个心跳间隔最多发送一条复制消息。 它还探讨了关注者的实际进度。
	//
	// When in StateReplicate, leader optimistically increases next
	// to the latest entry sent after sending replication message. This is
	// an optimized state for fast replicating log entries to the follower.
	//在StateReplicate中时，领导者乐观地增加在发送复制消息后发送的最新条目旁边。 这是用于将日志条目快速复制到关注者的优化状态。
	//
	// When in StateSnapshot, leader should have sent out snapshot
	// before and stops sending any replication message.
	//在StateSnapshot中时，领导者应该已经发送了快照，然后停止发送任何复制消息。
	//
	// 此处顺便把StateType这个类型详细说明一下，StateType的代码在go.etcd.io/etcd/raft/tracker/state.go
	// Progress一共有三种状态，分别为探测（StateProbe）、复制（StateReplicate）、快照（StateSnapshot）
	// 探测：一般是系统选举完成后，Leader不知道所有Follower都是什么进度，所以需要发消息探测一下，从
	//    Follower的回复消息获取进度。在还没有收到回消息前都还是探测状态。因为不确定Follower是
	//    否活跃，所以发送太多的探测消息意义不大，只发送一个探测消息即可。
	// 复制：当Peer回复探测消息后，消息中有该节点接收的最大日志索引，如果回复的最大索引大于Match，
	//    以此索引更新Match，Progress就进入了复制状态，开启高速复制模式。复制制状态不同于
	//    探测状态，Leader会发送更多的日志消息来提升IO效率，就是上面提到的异步发送。这里就要引入
	//    Inflight概念了，飞行中的日志，意思就是已经发送给Follower还没有被确认接收的日志数据，
	//    后面会有inflight介绍。
	// 快照：快照状态说明Follower正在复制Leader的快照
	State StateType

	// PendingSnapshot is used in StateSnapshot.
	// PendingSnapshot 被用在 StateSnapshot 中。
	// If there is a pending snapshot, the pendingSnapshot will be set to the
	// index of the snapshot. If pendingSnapshot is set, the replication process of
	// this Progress will be paused. raft will not resend snapshot until the pending one
	// is reported to be failed.
	// 在快照状态时，快照的索引值
	PendingSnapshot uint64

	// RecentActive is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	//如果进度最近处于活动状态，则LatestActive为true。 接收到来自相应关注者的任何消息表示进度处于活动状态。 选举超时后，RecentActive可以重置为false。
	// TODO(tbg): the leader should always have this set to true.
	// 变量名字就能看出来，表示Follower最近是否活跃，只要Leader收到任何一个消息就表示节点是最近
	// 是活跃的。如果新一轮的选举，那么新的Leader默认为都是不活跃的。
	RecentActive bool

	// ProbeSent is used while this follower is in StateProbe. When ProbeSent is
	// true, raft should pause sending replication message to this peer until
	// ProbeSent is reset. See ProbeAcked() and IsPaused().
	//当此follower位于StateProbe中时，将使用ProbeSent。
	//当ProbeSent为true时，raft将暂停向该对等方发送复制消息，直到重置ProbeSent。 请参见ProbeAcked（）和IsPaused（）。
	// 探测状态时才有用，表示探测消息是否已经发送了，如果发送了就不会再发了，避免不必要的IO。
	//探测状态通过ProbeSent控制探测消息的发送频率，复制状态下通过Inflights控制发送流量。
	ProbeSent bool

	// Inflights is a sliding window for the inflight messages.
	// Inflights是Inflights中消息的滑动窗口。
	// Each inflight message contains one or more log entries.
	//每条inflight中的消息均包含一个或多个日志条目。
	// The max number of entries per message is defined in raft config as MaxSizePerMsg.
	// 每个message的entries的最大数量被定义到 raft config中的 MaxSizePerMsg
	// Thus inflight effectively limits both the number of inflight messages
	// and the bandwidth each Progress can use.
	// 因此 inflight 非常有效的 限制了 inflight messages 的数量和每个Progress使用的带宽
	// When inflights is Full, no more message should be sent.
	// 当 inflights 满了，不会有更多的message会被send。
	// When a leader sends out a message, the index of the last
	// entry should be added to inflights. The index MUST be added
	// into inflights in order.
	// 当一个leader发送了一个消息，该消息的最后一个entry的index应该被加到inflights中。index必须被有序的加入到inflights中
	// When a leader receives a reply, the previous inflights should
	// be freed by calling inflights.FreeLE with the index of the last
	// received entry.
	// 当leader 接收到回复，之前的inflights应该通过调用inflights.FreeLe被free掉，用接收到的最后一个entry的index当做参数去free
	//
	// Inflight前面提到了，在复制状态有作用，后面有他的代码解析，此处只需要知道他是个限流的作用即可。
	// Leader不能无休止的向Follower发送日志，飞行中的日志量太大对网络和节点都是负担。而且一个日志
	// 丢失其后面的日志都要重发，所以过大的飞行中的日志失败后的重发成本也很大。
	Inflights *Inflights

	// IsLearner is true if this progress is tracked for a learner.
	// IsLearner 是true 如果该progress追踪的是个learner
	IsLearner bool
}

// ResetState moves the Progress into the specified State, resetting ProbeSent,
// PendingSnapshot, and Inflights.
// ResetState 将Progress设置到指定的State。重置 ProbeSent，PendingSnapshot 和 Inflights。
func (pr *Progress) ResetState(state StateType) {
	pr.ProbeSent = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.Inflights.reset()
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

// ProbeAcked is called when this peer has accepted an append. It resets
// ProbeSent to signal that additional append messages should be sent without
// further delay.
//当此对等方接受追加时，将调用ProbeAcked。 它会重置ProbeSent，以发出应发送附加附加消息的信号，而不会造成进一步延迟。
func (pr *Progress) ProbeAcked() {
	pr.ProbeSent = false
}

// BecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
// optionally and if larger, the index of the pending snapshot.
// BecomeProbe过渡到StateProbe。 将Next重置为Match + 1，或者重置为待处理快照的索引（如果较大）
func (pr *Progress) BecomeProbe() {
	// If the original state is StateSnapshot, progress knows that
	// the pending snapshot has been sent to this peer successfully, then
	// probes from pendingSnapshot + 1.
	if pr.State == StateSnapshot {
		pendingSnapshot := pr.PendingSnapshot
		pr.ResetState(StateProbe)
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.ResetState(StateProbe)
		pr.Next = pr.Match + 1
	}
}

// BecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
func (pr *Progress) BecomeReplicate() {
	pr.ResetState(StateReplicate)
	pr.Next = pr.Match + 1
}

// BecomeSnapshot moves the Progress to StateSnapshot with the specified pending
// snapshot index.
func (pr *Progress) BecomeSnapshot(snapshoti uint64) {
	pr.ResetState(StateSnapshot)
	pr.PendingSnapshot = snapshoti
}

// MaybeUpdate is called when an MsgAppResp arrives from the follower, with the
// index acked by it. The method returns false if the given n index comes from
// an outdated message. Otherwise it updates the progress and returns true.
// 当接收到一个从follower来的MsgAppResp类型的消息的时候，MaybeUpdate被调用。
// 如果给定的n索引来自过期消息，则该方法返回false。
// 否则，它将更新进度并返回true。
func (pr *Progress) MaybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
		pr.ProbeAcked()
	}
	pr.Next = max(pr.Next, n+1)
	return updated
}

// OptimisticUpdate signals that appends all the way up to and including index n
// are in-flight. As a result, Next is increased to n+1.
func (pr *Progress) OptimisticUpdate(n uint64) { pr.Next = n + 1 }

// MaybeDecrTo adjusts the Progress to the receipt of a MsgApp rejection. The
// arguments are the index the follower rejected to append to its log, and its
// last index.
//
// Rejections can happen spuriously as messages are sent out of order or
// duplicated. In such cases, the rejection pertains to an index that the
// Progress already knows were previously acknowledged, and false is returned
// without changing the Progress.
//
// If the rejection is genuine, Next is lowered sensibly, and the Progress is
// cleared for sending log entries.
func (pr *Progress) MaybeDecrTo(rejected, last uint64) bool {
	if pr.State == StateReplicate {
		// The rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
		if rejected <= pr.Match {
			return false
		}
		// Directly decrease next to match + 1.
		//
		// TODO(tbg): why not use last if it's larger?
		pr.Next = pr.Match + 1
		return true
	}

	// The rejection must be stale if "rejected" does not match next - 1. This
	// is because non-replicating followers are probed one entry at a time.
	if pr.Next-1 != rejected {
		return false
	}

	pr.Next = max(min(rejected, last+1), 1)
	pr.ProbeSent = false
	return true
}

// IsPaused returns whether sending log entries to this node has been throttled.
// This is done when a node has rejected recent MsgApps, is currently waiting
// for a snapshot, or has reached the MaxInflightMsgs limit. In normal
// operation, this is false. A throttled node will be contacted less frequently
// until it has reached a state in which it's able to accept a steady stream of
// log entries again.
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case StateProbe:
		return pr.ProbeSent
	case StateReplicate:
		return pr.Inflights.Full()
	case StateSnapshot:
		return true
	default:
		panic("unexpected state")
	}
}

func (pr *Progress) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%s match=%d next=%d", pr.State, pr.Match, pr.Next)
	if pr.IsLearner {
		fmt.Fprint(&buf, " learner")
	}
	if pr.IsPaused() {
		fmt.Fprint(&buf, " paused")
	}
	if pr.PendingSnapshot > 0 {
		fmt.Fprintf(&buf, " pendingSnap=%d", pr.PendingSnapshot)
	}
	if !pr.RecentActive {
		fmt.Fprintf(&buf, " inactive")
	}
	if n := pr.Inflights.Count(); n > 0 {
		fmt.Fprintf(&buf, " inflight=%d", n)
		if pr.Inflights.Full() {
			fmt.Fprint(&buf, "[full]")
		}
	}
	return buf.String()
}

// ProgressMap is a map of *Progress.
type ProgressMap map[uint64]*Progress

// String prints the ProgressMap in sorted key order, one Progress per line.
func (m ProgressMap) String() string {
	ids := make([]uint64, 0, len(m))
	for k := range m {
		ids = append(ids, k)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	var buf strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&buf, "%d: %s\n", id, m[id])
	}
	return buf.String()
}
