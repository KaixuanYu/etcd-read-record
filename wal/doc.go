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

/*
Package wal provides an implementation of a write ahead log that is used by
etcd.

package wal 提供了一种给etcd用的预写日志的实现

A WAL is created at a particular directory and is made up of a number of
segmented WAL files. Inside of each file the raft state and entries are appended
to it with the Save method:

WAL是在特定的目录下创建的，由许多分段的WAL文件组成。在每个文件的内部，raft状态和条目将用Save方法附加到其中：

	metadata := []byte{}
	w, err := wal.Create(zap.NewExample(), "/var/lib/etcd", metadata)
	...
	err := w.Save(s, ents)

After saving a raft snapshot to disk, SaveSnapshot method should be called to
record it. So WAL can match with the saved snapshot when restarting.

将raft快照保存到磁盘后，应该调用SaveSnapshot方法来记录它。所以WAL可以在重新启动时与保存的快照匹配。

	err := w.SaveSnapshot(walpb.Snapshot{Index: 10, Term: 2})

When a user has finished using a WAL it must be closed:

用完 wal 记得调用 Close 函数关闭。

	w.Close()

Each WAL file is a stream of WAL records. A WAL record is a length field and a wal record
protobuf. The record protobuf contains a CRC, a type, and a data payload. The length field is a
64-bit packed structure holding the length of the remaining logical record data in its lower
56 bits and its physical padding in the first three bits of the most significant byte. Each
record is 8-byte aligned so that the length field is never torn. The CRC contains the CRC32
value of all record protobufs preceding the current record.
每个 WAL 文件都是一个 WAL records 流。 一个WAL结构记录了一个长度字段和一个wal record 的protobuf。
wal record protobuf 包含 一个CRC字段，一个type字段 和一个 data 字段。
长度字段是一个64位打包的结构，在其低56位中保留其余逻辑记录数据的长度，并在最高有效字节的前三位中保留其物理填充。
每个记录都是8字节对齐的，因此长度字段永远不会被撕裂。 CRC包含当前记录之前的所有记录协议的CRC32值。

WAL files are placed inside of the directory in the following format:
$seq-$index.wal
WAL文件以以下格式放置在目录中：$seq- $index.wal

The first WAL file to be created will be 0000000000000000-0000000000000000.wal
indicating an initial sequence of 0 and an initial raft index of 0. The first
entry written to WAL MUST have raft index 0.
要创建的第一个WAL文件将是0000000000000000-0000000000000000.wal，指示初始序列0和初始raft索引0。写入WAL的第一个条目务必具有raft索引0。

WAL will cut its current tail wal file if its size exceeds 64MB. This will increment an internal
sequence number and cause a new file to be created. If the last raft index saved
was 0x20 and this is the first time cut has been called on this WAL then the sequence will
increment from 0x0 to 0x1. The new file will be: 0000000000000001-0000000000000021.wal.
If a second cut issues 0x10 entries with incremental index later then the file will be called:
0000000000000002-0000000000000031.wal.
如果WAL的大小超过64MB，WAL将剪切其当前的尾部wal文件。这将增加内部序列号并导致创建新文件。（$seq- $index.wal 就是$seq+1）
如果最后的raft索引是0x20并且是第一次剪切WAL文件，那么 sequence 将从 0x0 增长为 0x1. 新的文件将是0000000000000001-0000000000000021.wal.
如果第二次剪切之后发出0x10个带有增量索引的条目，则该文件将被称为：0000000000000002-0000000000000031.wal


At a later time a WAL can be opened at a particular snapshot. If there is no
snapshot, an empty snapshot should be passed in.
wal创建之后后就可以在特定的快照中打开WAL。如果没有快照，则应传入空快照。

	w, err := wal.Open("/var/lib/etcd", walpb.Snapshot{Index: 10, Term: 2})
	...

The snapshot must have been written to the WAL. 快照一定被写入到WAL中

Additional items cannot be Saved to this WAL until all of the items from the given
snapshot to the end of the WAL are read first:
在先读取从给定快照到WAL末尾的所有项目之前，无法将其他项目保存到此WAL：

	metadata, state, ents, err := w.ReadAll()

This will give you the metadata, the last raft.State and the slice of
raft.Entry items in the log.

*/
package wal
