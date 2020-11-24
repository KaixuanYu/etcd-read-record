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

package quorum

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

/*
 * MajorityConfig 多数Config？其实就是个key是 peer id 的 map（value无意义）。
 * map[peer.ID]none
 * 有两个功能：（核心就是找到大多数）
 * 1. 获取目前集群中 已提交的 index（CommittedIndex 函数）
 * 2. 唱票，判断传入的 vote，是 won lost 和 pedding
 */

// MajorityConfig is a set of IDs that uses majority quorums to make decisions.
// MajorityConfig是一组使用多数Quorum进行决策的ID。
type MajorityConfig map[uint64]struct{}

// 将一个MajorityConfig转化成字符串："( uint64 uint64 uint64)"
func (c MajorityConfig) String() string {
	sl := make([]uint64, 0, len(c))
	for id := range c {
		sl = append(sl, id)
	}
	sort.Slice(sl, func(i, j int) bool { return sl[i] < sl[j] })
	var buf strings.Builder // builder的效率更高
	buf.WriteByte('(')
	for i := range sl {
		if i > 0 {
			buf.WriteByte(' ')
		}
		fmt.Fprint(&buf, sl[i])
	}
	buf.WriteByte(')')
	return buf.String()
}

// Describe returns a (multi-line) representation of the commit indexes for the
// given lookuper.
// Describe返回给定lookuper的提交索引的（多行）表示形式。
// 只有单测有用到
func (c MajorityConfig) Describe(l AckedIndexer) string {
	if len(c) == 0 {
		return "<empty majority quorum>"
	}
	type tup struct {
		id  uint64
		idx Index
		ok  bool // idx found?
		bar int  // length of bar displayed for this tup 一个 []tup bar分别是0 1 2 3 4(根据index排序)
	}

	// Below, populate .bar so that the i-th largest commit index has bar i (we
	// plot this as sort of a progress bar). The actual code is a bit more
	// complicated and also makes sure that equal index => equal bar.
	//在下面填充.bar，以使第i个最大的提交索引具有bar i（我们将其绘制为进度条）。 实际的代码要复杂一些，并且还要确保等号=>等号。
	n := len(c)
	info := make([]tup, 0, n)
	for id := range c {
		// id = voterID
		// idx = Index
		// ok AckedIndex 是否有 该voterID
		idx, ok := l.AckedIndex(id)
		info = append(info, tup{id: id, idx: idx, ok: ok})
	}

	// Sort by index 根据index排序
	sort.Slice(info, func(i, j int) bool {
		if info[i].idx == info[j].idx {
			return info[i].id < info[j].id
		}
		return info[i].idx < info[j].idx
	})

	// Populate .bar. 填充 bar
	for i := range info {
		if i > 0 && info[i-1].idx < info[i].idx {
			info[i].bar = i
		}
	}

	// Sort by ID.
	sort.Slice(info, func(i, j int) bool {
		return info[i].id < info[j].id
	})

	var buf strings.Builder

	// Print.
	// 如果有3个 [ {id=1,idx=100,bar=1} {id=2,idx=101,bar=2} {id=3,idx=99,bar=0}]
	//x>     100    (id=1)
	//xx>    101    (id=2)
	//>       99    (id=3)
	//100
	fmt.Fprint(&buf, strings.Repeat(" ", n)+"    idx\n")
	for i := range info {
		bar := info[i].bar
		if !info[i].ok {
			fmt.Fprint(&buf, "?"+strings.Repeat(" ", n))
		} else {
			fmt.Fprint(&buf, strings.Repeat("x", bar)+">"+strings.Repeat(" ", n-bar))
		}
		fmt.Fprintf(&buf, " %5d    (id=%d)\n", info[i].idx, info[i].id)
	}
	return buf.String()
}

// Slice returns the MajorityConfig as a sorted slice.
// Slice 返回排好序的 MajorityConfig
func (c MajorityConfig) Slice() []uint64 {
	var sl []uint64
	for id := range c {
		sl = append(sl, id)
	}
	sort.Slice(sl, func(i, j int) bool { return sl[i] < sl[j] })
	return sl
}

// 插入排序
func insertionSort(sl []uint64) {
	a, b := 0, len(sl)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && sl[j] < sl[j-1]; j-- {
			sl[j], sl[j-1] = sl[j-1], sl[j]
		}
	}
}

// CommittedIndex computes the committed index from those supplied via the
// provided AckedIndexer (for the active config).
// CommittedIndex从通过提供的AckedIndexer（用于活动配置）提供的索引中计算承诺索引。
// 就是返回可以commit的index，比如有3个节点，都建康。每个节点的commit的index分别是 1 4 7
// 那么返回 4
// 如果是 1 4 7 9 也返回4
func (c MajorityConfig) CommittedIndex(l AckedIndexer) Index {
	n := len(c)
	if n == 0 {
		// This plays well with joint quorums which, when one half is the zero
		// MajorityConfig, should behave like the other half.
		return math.MaxUint64
	}

	// Use an on-stack slice to collect the committed indexes when n <= 7
	// (otherwise we alloc). The alternative is to stash a slice on
	// MajorityConfig, but this impairs usability (as is, MajorityConfig is just
	// a map, and that's nice). The assumption is that running with a
	// replication factor of >7 is rare, and in cases in which it happens
	// performance is a lesser concern (additionally the performance
	// implications of an allocation here are far from drastic).
	// 下面的代码对理解函数的实现原理没有多大影响，只是用了一个小技巧，在Peer数量不大于7个的情况下
	// 优先用栈数组，否则通过堆申请内存。因为raft集群超过7个的概率不大，用栈效率会更高
	var stk [7]uint64
	var srt []uint64
	if len(stk) >= n {
		srt = stk[:n]
	} else {
		srt = make([]uint64, n)
	}

	{ //这个大括号应该是为了保护i这个值
		// Fill the slice with the indexes observed. Any unused slots will be
		// left as zero; these correspond to voters that may report in, but
		// haven't yet. We fill from the right (since the zeroes will end up on
		// the left after sorting below anyway).
		i := n - 1
		for id := range c {
			if idx, ok := l.AckedIndex(id); ok {
				srt[i] = uint64(idx)
				i--
			}
		}
	}

	// Sort by index. Use a bespoke algorithm (copied from the stdlib's sort
	// package) to keep srt on the stack.
	insertionSort(srt)

	// The smallest index into the array for which the value is acked by a
	// quorum. In other words, from the end of the slice, move n/2+1 to the
	// left (accounting for zero-indexing).
	pos := n - (n/2 + 1)
	return Index(srt[pos])
}

// VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
// a result indicating whether the vote is pending (i.e. neither a quorum of
// yes/no has been reached), won (a quorum of yes has been reached), or lost (a
// quorum of no has been reached).
func (c MajorityConfig) VoteResult(votes map[uint64]bool) VoteResult {
	if len(c) == 0 {
		// By convention, the elections on an empty config win. This comes in
		// handy with joint quorums because it'll make a half-populated joint
		// quorum behave like a majority quorum.
		//按照惯例，空配置上的选举获胜。 这对于联合法定人数派上用场，因为它将使半填充的联合法定人数表现为多数法定人数。
		// 空配置直接获胜，因为超过半数同意
		return VoteWon
	}

	ny := [2]int{} // vote counts for no and yes, respectively 投票分别代表否和赞成

	var missing int
	for id := range c {
		v, ok := votes[id]
		if !ok {
			missing++
			continue
		}
		if v {
			ny[1]++
		} else {
			ny[0]++
		}
	}

	q := len(c)/2 + 1
	if ny[1] >= q {
		return VoteWon
	}
	if ny[1]+missing >= q {
		return VotePending
	}
	return VoteLost
}
