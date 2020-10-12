---
title: Performance [性能]
---

## Understanding performance [理解性能]

etcd provides stable, sustained high performance. Two factors define performance: latency and throughput. Latency is the time taken to complete an operation. Throughput is the total operations completed within some time period. Usually average latency increases as the overall throughput increases when etcd accepts concurrent client requests. In common cloud environments, like a standard `n-4` on Google Compute Engine (GCE) or a comparable machine type on AWS, a three member etcd cluster finishes a request in less than one millisecond under light load, and can complete more than 30,000 requests per second under heavy load.

etcd 提供稳定、持续的高性能。两个因素定义了性能：延迟和吞吐量。延迟是指完成一个操作需要花费的时间。吞吐量是一段时间内完成的全部操作数量。通常，当etcd接收并发的客户端请求时，平均延时会随着总体吞吐量的增加而增加。在常见的云环境中（like a standard n-4 on Google Compute Engine (GCE)，基本就是4核16G内存本地ssd磁盘），三成员的etcd集群在轻负载下可以在不到一毫秒的时间内完成请求，并且可以在重负载下每秒完成30000个请求。

etcd uses the Raft consensus algorithm to replicate requests among members and reach agreement. Consensus performance, especially commit latency, is limited by two physical constraints: network IO latency and disk IO latency. The minimum time to finish an etcd request is the network Round Trip Time (RTT) between members, plus the time `fdatasync` requires to commit the data to permanent storage. The RTT within a datacenter may be as long as several hundred microseconds. A typical RTT within the United States is around 50ms, and can be as slow as 400ms between continents. The typical fdatasync latency for a spinning disk is about 10ms. For SSDs, the latency is often lower than 1ms. To increase throughput, etcd batches multiple requests together and submits them to Raft. This batching policy lets etcd attain high throughput despite heavy load.

etcd 使用 raft 共识算法在成员之间复制请求并达成共识。共识性能（尤其是提交延时），受两个物理因素限制：网络IO延时和磁盘IO延时。完成etcd请求的最短时间是成员之间的网络往返时间（RTT），加上`fdatasync`将数据提交到永久储存所需的时间。数据中心内的RTT可能长达数百微妙。美国境内的典型RTT为50ms，两大洲之间的RTT可能慢至400ms，旋转磁盘的典型`fdatasync`延时约为10ms。对于ssd，延时通常低于1ms。为了提高吞吐量，etcd将多个请求一起批处理，然后将他们提交给raft。通过此批处理策略，尽管负载很重，etcd扔可实现高吞吐量。

There are other sub-systems which impact the overall performance of etcd. Each serialized etcd request must run through etcd’s boltdb-backed MVCC storage engine, which usually takes tens of microseconds to finish. Periodically etcd incrementally snapshots its recently applied requests, merging them back with the previous on-disk snapshot. This process may lead to a latency spike. Although this is usually not a problem on SSDs, it may double the observed latency on HDD. Likewise, inflight compactions can impact etcd’s performance. Fortunately, the impact is often insignificant since the compaction is staggered so it does not compete for resources with regular requests. The RPC system, gRPC, gives etcd a well-defined, extensible API, but it also introduces additional latency, especially for local reads.

还有其他子系统会影响etcd的整体性能。每个序列化的etcd请求都必须通过etcd由boltdb支持的mvcc储存引擎运行，这通常需要数10微妙才能完成。定期etcd增量快照其最近应用的请求，并将其与先前的磁盘快照合并。此过程可能会导致一个延迟峰值。尽管这通常不是ssd的问题，但它可能会使在hdd上观察到的延迟增加一倍。同样，运行中的压缩也会影响etcd性能。幸运的是，由于压缩是交错进行的，因此它不会与常规请求竞争资源，因此影响通常不大。RPC系统gRPC为etcd提供了一个定义良好、可扩展的API，但它也引入了额外的延迟，特别是对于本地读取。

## Benchmarks [压测]

Benchmarking etcd performance can be done with the [benchmark](https://github.com/coreos/etcd/tree/master/tools/benchmark) CLI tool included with etcd.

可以使用etcd附带的benchmarkcli工具对etcd性能进行基准测试。

For some baseline performance numbers, we consider a three member etcd cluster with the following hardware configuration:

对于一些基准性能数字，我们考虑使用以下硬件配置的三成员etcd集群：

- Google Cloud Compute Engine 谷歌云计算引擎
- 3 machines of 8 vCPUs + 16GB Memory + 50GB SSD 3台8核虚拟cpu16内存50G SSD 机器
- 1 machine(client) of 16 vCPUs + 30GB Memory + 50GB SSD 一台16核虚拟cpu 30G内存 50G SSD 客户端
- Ubuntu 17.04 系统
- etcd 3.2.0, go 1.8.3 etcd和go版本

With this configuration, etcd can approximately write:
以上配置，etcd 的表现如下：

| 键的个数 | 键大小 bytes | 值大小 bytes | 连接数 | 客户端数 | 目标 etcd server | 平均写 QPS | 平均每秒延时 | Average server RSS （RSS是内存使用情况） |
|---------------:|------------------:|--------------------:|----------------------:|------------------:|--------------------|------------------:|----------------------------:|-------------------:|
| 10,000 | 8 | 256 | 1 | 1 | leader only | 583 | 1.6ms | 48 MB |
| 100,000 | 8 | 256 | 100 | 1000 | leader only | 44,341 | 22ms |  124MB |
| 100,000 | 8 | 256 | 100 | 1000 | all members |  50,104 | 20ms |  126MB |

Sample commands are:

示例命令如下：

```sh
# write to leader
# 往 leader 写
benchmark --endpoints=${HOST_1} --target-leader --conns=1 --clients=1 \
    put --key-size=8 --sequential-keys --total=10000 --val-size=256
benchmark --endpoints=${HOST_1} --target-leader  --conns=100 --clients=1000 \
    put --key-size=8 --sequential-keys --total=100000 --val-size=256

# write to all members
# 往所有成员写
benchmark --endpoints=${HOST_1},${HOST_2},${HOST_3} --conns=100 --clients=1000 \
    put --key-size=8 --sequential-keys --total=100000 --val-size=256
```

Linearizable read requests go through a quorum of cluster members for consensus to fetch the most recent data. Serializable read requests are cheaper than linearizable reads since they are served by any single etcd member, instead of a quorum of members, in exchange for possibly serving stale data. etcd can read: 


可线性化的读取请求经过集群成员的仲裁，以达成共识以获取最新数据。可序列化的读取请求比线性化的读取便宜，因为可序列化的读取请求由任何单个etcd成员（而不是法定成员）提供，代价是可能读取到陈旧的数据。 etcd可以读取：

这段的意思是，etcd有两种读请求方式（可配置的）：linearizable read 和 serializable read，linearizable read 是说，读请求也要得到集群大多数节点的同意。而 serializable read 是请求到哪个节点就返回哪个节点的数据，这种方式可能会因为一些网络分区的问题读到旧的数据。

| Number of requests 请求数 | Key size in bytes 键大小 | Value size in bytes 值大小 | Number of connections 连接数 | Number of clients 客户端数 | Consistency 一致性 | Average read QPS 读平均qps | Average latency per request 每个请求的平均延时 |
|-------------------:|------------------:|--------------------:|----------------------:|------------------:|-------------|-----------------:|----------------------------:|
| 10,000 | 8 | 256 | 1 | 1 | Linearizable | 1,353 | 0.7ms |
| 10,000 | 8 | 256 | 1 | 1 | Serializable | 2,909 | 0.3ms |
| 100,000 | 8 | 256 | 100 | 1000 | Linearizable | 141,578 | 5.5ms |
| 100,000 | 8 | 256 | 100 | 1000 | Serializable | 185,758 | 2.2ms |

Sample commands are:

示例命令如下：

```sh
# Single connection read requests 单连接读请求
benchmark --endpoints=${HOST_1},${HOST_2},${HOST_3} --conns=1 --clients=1 \
    range YOUR_KEY --consistency=l --total=10000
benchmark --endpoints=${HOST_1},${HOST_2},${HOST_3} --conns=1 --clients=1 \
    range YOUR_KEY --consistency=s --total=10000

# Many concurrent read requests 多个并发读请求
benchmark --endpoints=${HOST_1},${HOST_2},${HOST_3} --conns=100 --clients=1000 \
    range YOUR_KEY --consistency=l --total=100000
benchmark --endpoints=${HOST_1},${HOST_2},${HOST_3} --conns=100 --clients=1000 \
    range YOUR_KEY --consistency=s --total=100000
```

We encourage running the benchmark test when setting up an etcd cluster for the first time in a new environment to ensure the cluster achieves adequate performance; cluster latency and throughput can be sensitive to minor environment differences.


我们鼓励在新环境中首次设置etcd集群时运行基准测试，以确保该集群获得足够的性能；群集延迟和吞吐量可能会对较小的环境差异敏感。