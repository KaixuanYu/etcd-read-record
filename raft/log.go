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

import (
	"fmt"
	"log"

	pb "go.etcd.io/etcd/v3/raft/raftpb"
)

type raftLog struct {
	// storage contains all stable entries since the last snapshot.
	// 存储包含自上次快照以来的所有稳定条目。
	storage Storage

	// unstable contains all unstable entries and snapshot.
	// they will be saved into storage.
	// 不稳定包含所有不稳定的条目和快照。 它们将被保存到存储中。
	// 已经生成或者发出，但是还没有得到集群认可的日志
	unstable unstable

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	//已获得集群至少二分之一成员认可的日志
	// todo 也可能 committed 额applied 跟storage 和unstable是没啥关系的呢？
	committed uint64
	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// // apply是已指示应用程序将其应用于状态机的最高日志位置。
	////不变式：已应用<=已提交
	// 已经被应用的日志
	applied uint64

	logger Logger // 打日志用的

	// maxNextEntsSize is the maximum number aggregate byte size of the messages
	// returned from calls to nextEnts.
	// maxNextEntsSize是从对nextEnts的调用返回的消息的最大总数字节大小。
	maxNextEntsSize uint64
}

// newLog returns log using the given storage and default options. It
// recovers the log to the state that it just commits and applies the
// latest snapshot.
// newLog使用给定的存储空间和默认选项返回日志。 它将日志恢复到刚提交并应用最新快照的状态。
func newLog(storage Storage, logger Logger) *raftLog {
	return newLogWithSize(storage, logger, noLimit)
}

// newLogWithSize returns a log using the given storage and max
// message size.
func newLogWithSize(storage Storage, logger Logger, maxNextEntsSize uint64) *raftLog {
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	log := &raftLog{
		storage:         storage,
		logger:          logger,
		maxNextEntsSize: maxNextEntsSize,
	}
	// 如果 storage 存了5个entry，他们的index分别是 3 4 5 6 7
	// firstIndex = 4
	// lastIndex = 7
	// log.unstable.offset = 8
	// log.committed = 3
	// log.applied = 3
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	log.unstable.offset = lastIndex + 1
	log.unstable.logger = logger
	// Initialize our committed and applied pointers to the time of the last compaction.
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

	return log
}

func (l *raftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d", l.committed, l.applied, l.unstable.offset, len(l.unstable.entries))
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
// maybeAppend 返回 0，false 如果entries不能追加，否则，返回 entries的最后一个index， true
//  1 2 3 4 5 6 7 8 ， 1 2 commit了，3 4 5 在storage中， 6 7 8 在unstable中
// 接下来要插入的大概率是 9 10。这个不确定。。。
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		//  1 2 3 4 5 6 7 8 ， 1 2 commit了，3 4 5 在storage中， 6 7 8 在unstable中
		//  参数是index=8， logTerm 匹配 ， committed = 10， ents = 8 9 10
		//  lastnewi = 10
		// ci = 9 这里假设8的term都是3，那么ci返回的就是log中第一个没有的entry的index，所以等于9
		//  走default，默认去append了 9 10 到log的unstable中。
		// 然后移动commit
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			l.logger.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := index + 1
			l.append(ents[ci-offset:]...)
		}
		// 看这里的意思，l.committed 中 存的是已经放入 log 中的最后一个entry 的 index，跟lastIndex一样 ？应该不是，是min，不是max
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

// 返回的参数是追加之后的最后一个entry的index（这里是装入unstable中）
func (l *raftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		// 如果不需要追加就返回目前最后一个index
		return l.lastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		l.logger.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	// 将 ents 装入 unstable 中
	l.unstable.truncateAndAppend(ents)
	return l.lastIndex()
}

// findConflict finds the index of the conflict.
// findConflict 找到 conflict 的index
// It returns the first pair of conflicting entries between the existing
// entries and the given entries, if there are any.
// 如果存在，则返回现有条目和给定条目之间的第一对冲突条目。
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// 如果没有冲突的entries，并且存在的entries包含所有给定的entries，返回0
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// 如果 没有冲突的entries，但是给定的entries中有新的 entries， 那么新entry的第一个index将被返回
// An entry is considered to be conflicting if it has the same index but
// a different term.
// 如果条目具有相同的index但term不同，则认为该条目存在冲突。
// The first entry MUST have an index equal to the argument 'from'.
//第一个条目必须具有等于自变量'from'的索引。
// The index of the given entries MUST be continuously increasing.
// 给定条目的index必须是递增的。
func (l *raftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.lastIndex() {
				l.logger.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

// 返回unstable的entries
func (l *raftLog) unstableEntries() []pb.Entry {
	if len(l.unstable.entries) == 0 {
		return nil
	}
	return l.unstable.entries
}

// nextEnts returns all the available entries for execution.
// If applied is smaller than the index of snapshot, it returns all committed
// entries after the index of snapshot.
// nextEnts返回所有可用的条目以供执行。
//如果应用的大小小于快照的索引，它将返回快照索引之后的所有已提交条目。
//新：就是拿出已经commit但是未apply的ents。
func (l *raftLog) nextEnts() (ents []pb.Entry) {
	off := max(l.applied+1, l.firstIndex())
	if l.committed+1 > off {
		ents, err := l.slice(off, l.committed+1, l.maxNextEntsSize)
		if err != nil {
			l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

// hasNextEnts returns if there is any available entries for execution. This
// is a fast check without heavy raftLog.slice() in raftLog.nextEnts().
func (l *raftLog) hasNextEnts() bool {
	off := max(l.applied+1, l.firstIndex())
	return l.committed+1 > off
}

// hasPendingSnapshot returns if there is pending snapshot waiting for applying.
func (l *raftLog) hasPendingSnapshot() bool {
	return l.unstable.snapshot != nil && !IsEmptySnap(*l.unstable.snapshot)
}

func (l *raftLog) snapshot() (pb.Snapshot, error) {
	if l.unstable.snapshot != nil {
		return *l.unstable.snapshot, nil
	}
	return l.storage.Snapshot()
}

// 这个如果 raftLog.unstable.snapshot 有，那么就返回snapshot的index
// 如果没有 就返回 storage 的第二个 entry
func (l *raftLog) firstIndex() uint64 {
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		return i
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return index
}

// 返回 log 中的最后一个index。 如果有unstable，就是unstable的最后一个index
// 如果unstable中没有东西，那么就是storage中的最后一个index
func (l *raftLog) lastIndex() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return i
}

// 只是更新了一下 raftLog.committed
func (l *raftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.lastIndex() < tocommit {
			l.logger.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.lastIndex())
		}
		l.committed = tocommit
	}
}

// 1 2[applied] 3 4[committed] [5 6 7]storage [8 9 10]unstable 可能是这种情况啊。如果storage的确是内存缓存的话。
func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	// applied<=i<=committed 1 2 3[applied] 4 5[committed] 6 7
	// 意思就是只能applied，committed了的，但是已经applied的，也不用applied了
	if l.committed < i || i < l.applied {
		l.logger.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	//将applied更新到i
	l.applied = i
}

// 这也只是更新了一下unstable的offset和entry
func (l *raftLog) stableTo(i, t uint64) { l.unstable.stableTo(i, t) }

//就更新了一下 unstable的snapshot=nil
func (l *raftLog) stableSnapTo(i uint64) { l.unstable.stableSnapTo(i) }

//  返回最后一个entry的term
func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		l.logger.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

// 返回指定entry的term
func (l *raftLog) term(i uint64) (uint64, error) {
	// the valid term range is [index of dummy entry, last index]
	dummyIndex := l.firstIndex() - 1
	if i < dummyIndex || i > l.lastIndex() {
		// TODO: return an error instead?
		return 0, nil
	}

	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}

	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err) // TODO(bdarnell)
}

// 返回index=i到最后一个entry的所欲entry，最大返回maxsize个
func (l *raftLog) entries(i, maxsize uint64) ([]pb.Entry, error) {
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxsize)
}

// allEntries returns all entries in the log.
// 返回log中的所有的entries
func (l *raftLog) allEntries() []pb.Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if err == ErrCompacted { // try again if there was a racing compaction
		return l.allEntries()
	}
	// TODO (xiangli): handle error?
	panic(err)
}

// isUpToDate determines if the given (lastIndex,term) log is more up-to-date
// by comparing the index and term of the last entries in the existing logs.
// If the logs have last entries with different terms, then the log with the
// later term is more up-to-date. If the logs end with the same term, then
// whichever log has the larger lastIndex is more up-to-date. If the logs are
// the same, the given log is up-to-date.
func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

func (l *raftLog) matchTerm(i, term uint64) bool {
	t, err := l.term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *raftLog) restore(s pb.Snapshot) {
	l.logger.Infof("log [%s] starts to restore snapshot [index: %d, term: %d]", l, s.Metadata.Index, s.Metadata.Term)
	l.committed = s.Metadata.Index
	l.unstable.restore(s)
}

// slice returns a slice of log entries from lo through hi-1, inclusive.
// slice 返回 [lo，hi) 范围内的 log 的entries
// 就是从 storage 和 unstable 中找。
func (l *raftLog) slice(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry
	if lo < l.unstable.offset {
		storedEnts, err := l.storage.Entries(lo, min(hi, l.unstable.offset), maxSize)
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			l.logger.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.unstable.offset))
		} else if err != nil {
			panic(err) // TODO(bdarnell)
		}

		// check if ents has reached the size limitation
		if uint64(len(storedEnts)) < min(hi, l.unstable.offset)-lo {
			return storedEnts, nil
		}

		ents = storedEnts
	}
	if hi > l.unstable.offset {
		unstable := l.unstable.slice(max(lo, l.unstable.offset), hi)
		if len(ents) > 0 {
			combined := make([]pb.Entry, len(ents)+len(unstable))
			n := copy(combined, ents)
			copy(combined[n:], unstable)
			ents = combined
		} else {
			ents = unstable
		}
	}
	return limitSize(ents, maxSize), nil
}

// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.lastIndex() + 1 - fi
	if lo < fi || hi > fi+length {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex())
	}
	return nil
}

func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Panicf("unexpected error (%v)", err)
	return 0
}
