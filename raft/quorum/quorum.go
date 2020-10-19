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
	"math"
	"strconv"
)

// Index is a Raft log position.
// Index 是 Raft log 的位置
type Index uint64

func (i Index) String() string {
	if i == math.MaxUint64 { //这些极值都在math包，之前都是自己写的...
		return "∞"
	}
	return strconv.FormatUint(uint64(i), 10)
}

// AckedIndexer allows looking up a commit index for a given ID of a voter
// from a corresponding MajorityConfig.
// AckedIndexer允许从对应的MajorityConfig中查找投票者给定ID的提交索引。
type AckedIndexer interface {
	AckedIndex(voterID uint64) (idx Index, found bool)
}

// map中key是voterID value是Index
type mapAckIndexer map[uint64]Index

// 返回 map 中 voterID 的 Index
func (m mapAckIndexer) AckedIndex(id uint64) (Index, bool) {
	idx, ok := m[id]
	return idx, ok
}

// VoteResult indicates the outcome of a vote. VoteResult 标识表决结果
//
//go:generate stringer -type=VoteResult
type VoteResult uint8

const (
	// VotePending indicates that the decision of the vote depends on future
	// votes, i.e. neither "yes" or "no" has reached quorum yet.
	// VotePending表示投票决定取决于将来的投票，即“是”或“否”尚未达到法定人数。
	VotePending VoteResult = 1 + iota
	// VoteLost indicates that the quorum has voted "no". VoteLost表示仲裁已投票赞成“no”。
	VoteLost
	// VoteWon indicates that the quorum has voted "yes". VoteWon表示仲裁已投票赞成“yes”。
	VoteWon
)
