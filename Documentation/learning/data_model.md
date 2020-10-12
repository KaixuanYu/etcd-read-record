---
title: Data model
---

etcd is designed to reliably store infrequently updated data and provide reliable watch queries. etcd exposes previous versions of key-value pairs to support inexpensive snapshots and watch history events (“time travel queries”). A persistent, multi-version, concurrency-control data model is a good fit for these use cases.

etcd旨在可靠地存储不经常更新的数据并提供可靠的监视查询。 etcd公开了键值对的先前版本，以支持廉价的快照和观看历史事件（“时间旅行查询”）。 持久的多版本并发控制数据模型非常适合这些用例。

etcd stores data in a multiversion [persistent][persistent-ds] key-value store. The persistent key-value store preserves the previous version of a key-value pair when its value is superseded with new data. The key-value store is effectively immutable; its operations do not update the structure in-place, but instead always generate a new updated structure. All past versions of keys are still accessible and watchable after modification. To prevent the data store from growing indefinitely over time and from maintaining old versions, the store may be compacted to shed the oldest versions of superseded data.

etcd将数据存储在多版本[persistent] [persistent-ds]键值存储中。 当持久性键值存储的值替换为新数据时，它会保留键值对的先前版本。 键值存储实际上是不可变的。 其操作不会就地更新结构，而是始终生成新的更新结构。 修改后，所有以前的key版本仍然可以访问和观看。 为防止数据存储随时间无限期增长并维护旧版本，可以压缩存储以释放掉最旧版本的取代数据

### Logical view [逻辑视图]

The store’s logical view is a flat binary key space. The key space has a lexically sorted index on byte string keys so range queries are inexpensive.

储存的逻辑视图是平面的二进制键空间。 键空间在字节字符串键上具有按词法排序的索引，因此范围查询代价很低。

The key space maintains multiple **revisions**. When the store is created, the initial revision is 1. Each atomic mutative operation (e.g., a transaction operation may contain multiple operations) creates a new revision on the key space. All data held by previous revisions remains unchanged. Old versions of key can still be accessed through previous revisions. Likewise, revisions are indexed as well; ranging over revisions with watchers is efficient. If the store is compacted to save space, revisions before the compact revision will be removed. Revisions are monotonically increasing over the lifetime of a cluster.

键空间维护多个 **revisions**. 当储存建立之后，出事 revision 是 1 。 每个原子的更新操作（例如：一个操作事务可能包含多个操作）会在键空间上创建一个新的 revision 。 之前的 revisions 保存的数据都不会发生改变。旧版本的key仍然可以通过之前的 revisions 访问到。同样的， revision 也会被索引；用观察者对修订进行调整是有效的（没懂）。如果为了节省空间对存储进行了压缩，那么压缩revision之前的revisions会被移除。在集群的整个生命周期中，revisions都是单调递增的。

A key's life spans a generation, from creation to deletion. Each key may have one or multiple generations. Creating a key increments the **version** of that key, starting at 1 if the key does not exist at the current revision. Deleting a key generates a key tombstone, concluding the key’s current generation by resetting its version to 0. Each modification of a key increments its version; so, versions are monotonically increasing within a key's generation. Once a compaction happens, any generation ended before the compaction revision will be removed, and values set before the compaction revision except the latest one will be removed.

从创建都删除，key的生命跨越了generation（一代）。每个key都可能有一个或者多个 generation 。创建一个key会增加该key的 **version** ，如果在当前的 revision 不存在该key，那么version从1开始。删除一个key会生成一个 key tombstone（key墓碑） ，通过将key的version设置为0来结束key的这一代（当前的generation，一次generation就是version从1到0的过程，比如1,2,3,0）。key每次修改都会增加其version，所以version会在一代（该generation）中递增。如果发生压缩，那么在压缩revision之前的generation都会被移除，并且将删除压缩revision之前设置的值（最新的除外）。

### Physical view

etcd stores the physical data as key-value pairs in a persistent [b+tree][b+tree]. Each revision of the store’s state only contains the delta from its previous revision to be efficient. A single revision may correspond to multiple keys in the tree.

The key of key-value pair is a 3-tuple (major, sub, type). Major is the store revision holding the key. Sub differentiates among  keys within the same revision. Type is an optional suffix for special value (e.g., `t` if the value contains a tombstone). The value of the key-value pair contains the modification from previous revision, thus one delta from previous revision. The b+tree is ordered by key in lexical byte-order. Ranged lookups over revision deltas are fast; this enables quickly finding modifications from one specific revision to another. Compaction removes out-of-date keys-value pairs.

etcd also keeps a secondary in-memory [btree][btree] index to speed up range queries over keys. The keys in the btree index are the keys of the store exposed to user. The value is a pointer to the modification of the persistent b+tree. Compaction removes dead pointers.

[persistent-ds]: https://en.wikipedia.org/wiki/Persistent_data_structure
[btree]: https://en.wikipedia.org/wiki/B-tree
[b+tree]: https://en.wikipedia.org/wiki/B%2B_tree
