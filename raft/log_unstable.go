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

package raft

import pb "go.etcd.io/etcd/v3/raft/raftpb"

// unstable.entries[i] has raft log position i+unstable.offset.
// stable.entries [i]具有raft日志位置i + unstable.offset。
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
// 注意，unstable.offset可能小于storage中的最高日志位置； 这意味着在下一次写入存储之前，可能需要截断日志，然后才能保持unstable.entries。
type unstable struct {
	// the incoming unstable snapshot, if any.
	snapshot *pb.Snapshot
	// all entries that have not yet been written to storage.
	// 没有写入 storage 的所有的 entries
	entries []pb.Entry
	offset  uint64

	logger Logger
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
// todo 返回的是 snapshot 的index？
// todo 正常应该是一堆的entry，然后有个snapshot[这个snapshot就是对前面一堆的entry的一个快照]，接下来又是一堆entry，然后一个snapshot，类推。
// todo 所以如果有snapshot,它一定比第一个snapshot的index小。如果没有snapshot，那么就没有其他entry
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
// 如果有entry，返回 offset + len(ent) - 1
// 没有entry 有snapshot 返回 snapshot.Metadata.Index
// 否则返回0
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	return 0, false
}

// maybeTerm returns the term of the entry at index i, if there
// is any.
// 返回index=i的entry的Term
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}

	return u.entries[i-u.offset].Term, true
}

// 这个函数就是把i指定的index之前的entry给删了。不保存在unstable了
// 其实就是已经保存到永久存储了，unstable需要更新
func (u *unstable) stableTo(i, t uint64) {
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	// if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
	if gt == t && i >= u.offset {
		u.entries = u.entries[i+1-u.offset:]
		u.offset = i + 1
		u.shrinkEntriesArray()
	}
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
// shrinkEntriesArray 会丢弃entries slice使用的底层数组，如果大多数没有被使用的话。
//  这样可以避免保留对不再需要的大量潜在条目的引用。 仅清除条目将不安全，因为客户端可能仍在使用它们。
// 这个函数，就是判断，如果slice的len < cap/2 那么slice就缩容。
func (u *unstable) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	// 如果我们用的array的空间少于其空间的一半，我们就替换掉它。
	// 这个数字是相当任意的，选择它来尝试平衡内存使用量和分配数量。 可以通过一些集中的调整来改进它。
	const lenMultiple = 2
	if len(u.entries) == 0 {
		u.entries = nil
	} else if len(u.entries)*lenMultiple < cap(u.entries) {
		newEntries := make([]pb.Entry, len(u.entries))
		copy(newEntries, u.entries)
		u.entries = newEntries
	}
}

// 将snapshot持久化后更新清空snapshot，这里只清空，未持久化。
func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
	}
}

// 存个 snapshot
func (u *unstable) restore(s pb.Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.entries = nil
	u.snapshot = &s
}

// 截断并追加
func (u *unstable) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	//比如说一开始是 3 4 5 6
	// 第一个case是说如果参数ents是7 8 9 ... （第一个是7就行），就直接追加
	// 第二个case是说如果ents的第一个index<=3 直接赋值，更新offset
	// 默认是在中间，如果是5，那么久删除原有的 5 6 ，然后追加最新的 5 6 。
	switch {
	case after == u.offset+uint64(len(u.entries)):
		// after is the next index in the u.entries
		// directly append
		// ents的第一个正好是下一个需要加的，直接追加
		u.entries = append(u.entries, ents...)
	case after <= u.offset:
		u.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		u.offset = after
		u.entries = ents
	default:
		// truncate to after and copy to u.entries
		// then append
		// 正好处在中间，截断，再追加
		u.logger.Infof("truncate the unstable entries before index %d", after)
		u.entries = append([]pb.Entry{}, u.slice(u.offset, after)...)
		u.entries = append(u.entries, ents...)
	}
}

func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	u.mustCheckOutOfBounds(lo, hi)
	return u.entries[lo-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.entries)
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}
