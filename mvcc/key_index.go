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

package mvcc

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/google/btree"
	"go.uber.org/zap"
)

var (
	ErrRevisionNotFound = errors.New("mvcc: revision not found")
)

// keyIndex stores the revisions of a key in the backend.
// keyIndex 将 key 的 revisions 储存到 backend
// Each keyIndex has at least one key generation.
// 每个key Index 都至少有一个key的生命周期
// Each generation might have several key versions.
// 每个生命周期 可能有多个 key的versions
// Tombstone on a key appends an tombstone version at the end
// of the current generation and creates a new empty generation.
// 删除一个key 就是在 当前生命周期 给key增加一个 删除 version，并且创建一个新的空的生命周期
// Each version of a key has an index pointing to the backend.
// 每个 key 的 version 都有一个指向 backend 的 index
//
// For example: put(1.0);put(2.0);tombstone(3.0);put(4.0);tombstone(5.0) on key "foo"
// generate a keyIndex:
// key:     "foo"
// rev: 5
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {1.0, 2.0, 3.0(t)}
//
// Compact a keyIndex removes the versions with smaller or equal to
// rev except the largest one. If the generation becomes empty
// during compaction, it will be removed. if all the generations get
// removed, the keyIndex should be removed.
// 压缩一个keyIndex会删除除最大版本以外小于或等于rev的版本。 如果在压缩过程中 generation 成为了空，则将其删除。 如果所有 generations 都被删除，则应该删除keyIndex。
//
// For example:
// compact(2) on the previous example
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {2.0, 3.0(t)}
//
// compact(4)
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//
// compact(5):
// generations:
//    {empty} -> key SHOULD be removed.
//
// compact(6):
// generations:
//    {empty} -> key SHOULD be removed.
type keyIndex struct {
	key         []byte
	modified    revision // the main rev of the last modification
	generations []generation
}

// put puts a revision to the keyIndex.
// put 在keyIndex中加一个revision
func (ki *keyIndex) put(lg *zap.Logger, main int64, sub int64) {
	rev := revision{main: main, sub: sub}

	// 新增的revision必须大于之前的revision
	if !rev.GreaterThan(ki.modified) {
		lg.Panic(
			"'put' with an unexpected smaller revision",
			zap.Int64("given-revision-main", rev.main),
			zap.Int64("given-revision-sub", rev.sub),
			zap.Int64("modified-revision-main", ki.modified.main),
			zap.Int64("modified-revision-sub", ki.modified.sub),
		)
	}
	if len(ki.generations) == 0 {
		ki.generations = append(ki.generations, generation{})
	}
	g := &ki.generations[len(ki.generations)-1]
	if len(g.revs) == 0 { // create a new key
		keysGauge.Inc()
		g.created = rev
	}
	g.revs = append(g.revs, rev)
	g.ver++
	ki.modified = rev
}

// restore 就是用指定参数， 重新创建了 一个 generation ,然后追加到keyIndex的generations
func (ki *keyIndex) restore(lg *zap.Logger, created, modified revision, ver int64) {
	if len(ki.generations) != 0 {
		lg.Panic(
			"'restore' got an unexpected non-empty generations",
			zap.Int("generations-size", len(ki.generations)),
		)
	}

	ki.modified = modified
	g := generation{created: created, ver: ver, revs: []revision{modified}}
	ki.generations = append(ki.generations, g)
	keysGauge.Inc()
}

// tombstone puts a revision, pointing to a tombstone, to the keyIndex.
// It also creates a new empty generation in the keyIndex.
// It returns ErrRevisionNotFound when tombstone on an empty generation.
// tombstone 将指向 tombstone 的 revision 指向 keyIndex
// 它还在 keyIndex 中创建一个新的 空 generation
// 该函数就是在keyIndex的最后一个generation中加了一个软删除的revision，然后重新创建下一个generation
// 这个函数就是一个generation的结束，和另一个generation的开始
func (ki *keyIndex) tombstone(lg *zap.Logger, main int64, sub int64) error {
	if ki.isEmpty() {
		lg.Panic(
			"'tombstone' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}
	if ki.generations[len(ki.generations)-1].isEmpty() {
		return ErrRevisionNotFound
	}
	ki.put(lg, main, sub)
	ki.generations = append(ki.generations, generation{})
	keysGauge.Dec()
	return nil
}

// get gets the modified, created revision and version of the key that satisfies the given atRev.
// Rev must be higher than or equal to the given atRev.
// get满足给定atRev的密钥的修改，创建的修订版和版本。
// Rev必须高于或等于给定的atRev。
// 这个atRev 就是 revision 的 main 字段
func (ki *keyIndex) get(lg *zap.Logger, atRev int64) (modified, created revision, ver int64, err error) {
	if ki.isEmpty() {
		lg.Panic(
			"'get' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}
	//找到该revision所在的generation
	g := ki.findGeneration(atRev)
	if g.isEmpty() {
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}

	//找到generation中atRev所在的索引
	n := g.walk(func(rev revision) bool { return rev.main > atRev })
	if n != -1 {
		// 如果存在返回 第一个比 atRev大的revision。generation的第一个revision，和其对应的 version？
		return g.revs[n], g.created, g.ver - int64(len(g.revs)-n-1), nil
	}

	return revision{}, revision{}, 0, ErrRevisionNotFound
}

// since returns revisions since the given rev. Only the revision with the
// largest sub revision will be returned if multiple revisions have the same
// main revision.
// since 返回规定rev内的所有的revisions。只有拥有最大的sub revision的revision才会被返回，如果在有多个revision有相同的main revision的情况下
func (ki *keyIndex) since(lg *zap.Logger, rev int64) []revision {
	if ki.isEmpty() {
		lg.Panic(
			"'since' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}
	since := revision{rev, 0}
	var gi int
	// find the generations to start checking
	for gi = len(ki.generations) - 1; gi > 0; gi-- {
		g := ki.generations[gi]
		if g.isEmpty() {
			continue
		}
		if since.GreaterThan(g.created) {
			break
		}
	}

	var revs []revision
	var last int64
	// 这个循环把这个generation的所有的revision都装进 revs中了，只有碰到相同main revision的，只要最大的sub revision的那个。
	for ; gi < len(ki.generations); gi++ {
		for _, r := range ki.generations[gi].revs {
			if since.GreaterThan(r) {
				continue
			}
			if r.main == last {
				// replace the revision with a new one that has higher sub value,
				// because the original one should not be seen by external
				revs[len(revs)-1] = r
				continue
			}
			revs = append(revs, r)
			last = r.main
		}
	}
	return revs
}

// compact compacts a keyIndex by removing the versions with smaller or equal
// revision than the given atRev except the largest one (If the largest one is
// a tombstone, it will not be kept).
// If a generation becomes empty during compaction, it will be removed.
// compact通过删除修订版本小于或等于给定版本atRev的版本（最大的除外）来压缩keyIndex（如果最大的版本是墓碑，则不会保留）。
// 如果在压缩过程中一代变为空，它将被删除。
func (ki *keyIndex) compact(lg *zap.Logger, atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		lg.Panic(
			"'compact' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}

	genIdx, revIndex := ki.doCompact(atRev, available)

	g := &ki.generations[genIdx]
	if !g.isEmpty() {
		// remove the previous contents.
		if revIndex != -1 {
			// 如果generation中的revision没有遍历完，就将 revIndex 前面的revision都删除
			g.revs = g.revs[revIndex:]
		}
		// remove any tombstone
		// 这个判断条件，如果在删除完了之后,g.revs就只剩下一个了，那么这一个就是墓碑revision，（在该generation不是最后一个generation的情况下）
		// 然后删除墓碑，而且该generation也需要删除，所以genIdx++
		if len(g.revs) == 1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[0])
			genIdx++
		}
	}

	// remove the previous generations.
	// 删除之前的generation
	ki.generations = ki.generations[genIdx:]
}

// keep finds the revision to be kept if compact is called at given atRev.
// keep 找到 被保留下来的 revision，如果compact被在给定的atRev调用。
// keep 和 compact 的区别，就是compact 真正的删除了旧的，而keep做了compact除了删除以外的事情，就是返回了个available
func (ki *keyIndex) keep(atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		return
	}

	genIdx, revIndex := ki.doCompact(atRev, available)
	g := &ki.generations[genIdx]
	if !g.isEmpty() {
		// remove any tombstone
		// 如果available中有墓碑revision，就将墓碑revision删除
		// 这个判断的意思是，revIndex是一个generation中的最后一个（最后一个都是墓碑revision），
		// 但是不是最后一个generation的（因为最后一个generation还没结束，最后一个不是墓碑revision）
		if revIndex == len(g.revs)-1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[revIndex])
		}
	}
}

// 啥都没干，就是找到了generation的index和 该generation中revision的index
// 比如generation 有 1 2 3 4 5 6 （这是数是每个generation的最后一个墓碑revision的main revision） ，然后参数atRev=4，那么genIdx=5
// 然后遍历 5 的generation 的revs，将小于atRev的放进 available中，revIndex是第一个大于atRev的revision的index
func (ki *keyIndex) doCompact(atRev int64, available map[revision]struct{}) (genIdx int, revIndex int) {
	// walk until reaching the first revision smaller or equal to "atRev",
	// and add the revision to the available map
	f := func(rev revision) bool {
		if rev.main <= atRev {
			available[rev] = struct{}{}
			return false
		}
		return true
	}

	genIdx, g := 0, &ki.generations[0]
	// find first generation includes atRev or created after atRev
	for genIdx < len(ki.generations)-1 {
		if tomb := g.revs[len(g.revs)-1].main; tomb > atRev {
			break
		}
		genIdx++
		g = &ki.generations[genIdx]
	}

	// 这里 available 装了所有比 atRev小或者等于的revision，然后revIndex是第一个比atRev大的revision的索引
	revIndex = g.walk(f)

	return genIdx, revIndex
}

func (ki *keyIndex) isEmpty() bool {
	return len(ki.generations) == 1 && ki.generations[0].isEmpty()
}

// findGeneration finds out the generation of the keyIndex that the
// given rev belongs to. If the given rev is at the gap of two generations,
// which means that the key does not exist at the given rev, it returns nil.
// findGeneration 找出keyIndex的generation，该generation是给定的参数rev所属的。
// 如果给定的rev在两个generation中间，也就是说在给定的rev中没有key，它将返回nil
// 这个 参数 rev 是 revision.main
func (ki *keyIndex) findGeneration(rev int64) *generation {
	lastg := len(ki.generations) - 1
	cg := lastg

	for cg >= 0 {
		if len(ki.generations[cg].revs) == 0 {
			cg--
			continue
		}
		g := ki.generations[cg]
		if cg != lastg {
			if tomb := g.revs[len(g.revs)-1].main; tomb <= rev {
				return nil
			}
		}
		if g.revs[0].main <= rev {
			return &ki.generations[cg]
		}
		cg--
	}
	return nil
}

func (ki *keyIndex) Less(b btree.Item) bool {
	return bytes.Compare(ki.key, b.(*keyIndex).key) == -1
}

func (ki *keyIndex) equal(b *keyIndex) bool {
	if !bytes.Equal(ki.key, b.key) {
		return false
	}
	if ki.modified != b.modified {
		return false
	}
	if len(ki.generations) != len(b.generations) {
		return false
	}
	for i := range ki.generations {
		ag, bg := ki.generations[i], b.generations[i]
		if !ag.equal(bg) {
			return false
		}
	}
	return true
}

func (ki *keyIndex) String() string {
	var s string
	for _, g := range ki.generations {
		s += g.String()
	}
	return s
}

// generation contains multiple revisions of a key.
// generation 包含一个key的过多个 revisions
type generation struct {
	ver     int64    // ver 是一个数，记录了revs的长度？
	created revision // when the generation is created (put in first revision). 当 generation 被创建的时候，将第一个revision放入
	revs    []revision
}

func (g *generation) isEmpty() bool { return g == nil || len(g.revs) == 0 }

// walk walks through the revisions in the generation in descending order.
// It passes the revision to the given function.
// walk returns until: 1. it finishes walking all pairs 2. the function returns false.
// walk returns the position at where it stopped. If it stopped after
// finishing walking, -1 will be returned.
// walk 以降序浏览 generation 中的 revisions
// 它传递 revision 给给定的函数
// walk在以下情况会返回：1. 遍历完所有的revision 2 调用func返回false
// walk 返回停止的位置。如果遍历完所有的revision，就返回-1
func (g *generation) walk(f func(rev revision) bool) int {
	l := len(g.revs)
	for i := range g.revs {
		ok := f(g.revs[l-i-1])
		if !ok {
			return l - i - 1
		}
	}
	return -1
}

func (g *generation) String() string {
	return fmt.Sprintf("g: created[%d] ver[%d], revs %#v\n", g.created, g.ver, g.revs)
}

func (g generation) equal(b generation) bool {
	if g.ver != b.ver {
		return false
	}
	if len(g.revs) != len(b.revs) {
		return false
	}

	for i := range g.revs {
		ar, br := g.revs[i], b.revs[i]
		if ar != br {
			return false
		}
	}
	return true
}
