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

import "encoding/binary"

// revBytesLen is the byte length of a normal revision.
// revBytesLen 是一个正常的revision的字节长度
// First 8 bytes is the revision.main in big-endian format. The 9th byte
// is a '_'. The last 8 bytes is the revision.sub in big-endian format.
// 前 8 个字节 是 revision.main （以大端模式储存）。 第 9 位 是一个 '_' ，最后的8位代表 revision.sub （以大端模式储存）
const revBytesLen = 8 + 1 + 8

// A revision indicates modification of the key-value space.
// The set of changes that share same main revision changes the key-value space atomically.
// revision 代表 对键空间的修改
// 共享相同主修订的一组更改自动更改键值空间。
// 就是一个事务的修改都有同一个 main， 然后这个事务中有多个 改变操作，每个改变的操作的 sub 是递增的。revision就是这么个东西
type revision struct {
	// main is the main revision of a set of changes that happen atomically.
	// main 是原子发生的一组更改的 main revision
	main int64

	// sub is the sub revision of a change in a set of changes that happen
	// atomically. Each change has different increasing sub revision in that
	// set.
	// sub是原子发生的一组更改中的更改的子修订版。 每个更改在该集中具有不同的递增子修订。
	sub int64
}

// 比价 a 是否 大于 b
func (a revision) GreaterThan(b revision) bool {
	if a.main > b.main {
		return true
	}
	if a.main < b.main {
		return false
	}
	return a.sub > b.sub
}

func newRevBytes() []byte {
	return make([]byte, revBytesLen, markedRevBytesLen) // 8+1+8+1 一开始 前面 8+1+8个是默认值，最后一个1没填充
}

// 将 revision 转化为 byte 数组
func revToBytes(rev revision, bytes []byte) {
	binary.BigEndian.PutUint64(bytes, uint64(rev.main))
	bytes[8] = '_'
	binary.BigEndian.PutUint64(bytes[9:], uint64(rev.sub))
}

// 将 byte 数组转化为 revision
func bytesToRev(bytes []byte) revision {
	return revision{
		main: int64(binary.BigEndian.Uint64(bytes[0:8])),
		sub:  int64(binary.BigEndian.Uint64(bytes[9:])),
	}
}

// revision 数组
type revisions []revision

// 实现了排序用的东东~
func (a revisions) Len() int           { return len(a) }
func (a revisions) Less(i, j int) bool { return a[j].GreaterThan(a[i]) }
func (a revisions) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
