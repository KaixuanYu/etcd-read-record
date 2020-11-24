#### put流程梳理：
前提：1， 2， 3（leader）。目前committed的index=5

##### 第一步：往3里面put 一个数据。新的数据的index=6，就一个entry。

1. 3节点在stepLeader中接收到一个pb.MsgProp的消息：
2. 检查是否超过uncommitted的entry限定的的大小。
3. 将entry新append到raftlog.unstable中
4. 尝试更新3（本节点）节点的Progress，成功更新 match 到 6， next到7
5. 尝试commit，因为1,2节点目前的match是5，所以maybecommit失败，raftlog.committed 保持不变。
6. 组装 pb.MsgApp（追加entry消息类型） 消息往各个节点（1,2节点）发送。

pb.MsgApp消息具体结构如下:
```
pb.Message{
    Type:pb.MsgApp,
    Index:5,  //这个5是当前master节点认为该节点的next-1，因为没有发出但是未提交的，所以next是6，该index是5
    LogTerm:当前任期（暂时不考虑任期问题）,
    Entries:就那一个entry,
    Commit: 5, 
}
```


##### 第二步：从节点（1）接收到主节点（3节点）send来的pb.MsgApp消息
1. 判断pb.Message.Index是否小于当前节点已经commit的index，不小于，都是5.
2. 判断是否同一任期，是
3. 是否冲突，一般不冲突，这里1节点的raftlog中只到5，没有新的entry的6，所以不冲突返回0
4. commitTo(min(committed,lastnewi)) 这时候传过来的commit=5，lastnewi=6，所以commit保持不变。
5. 给leader发送 pb.MsgAppResp 类型的消息，其中 Index = 6 (就是新来的entry的最后一个index‘)
##### 第三步：主节点接收到从节点发送来的 pb.MsgAppResp
1. 将1节点（发送来的节点）的Progress的Match更新到6（传过来的index），next=index+1=7(调用的是progress.MaybeUpdate函数)
2. 判断当前1节点Progress的State，如果是StateReplicate，就将其inflights的index节点给free掉。（inflights类似速率控制器，如果发送给从节点，但是该节点还没commit回来的entry数量达到某个size，就停止继续发送MsgApp消息。）
3. 调用 maybeCommit， 这时候 1 3 节点的commit都到了 6，属于大多数到了6，就认为可以提交了，将leader的raftlog的committed更新到6
4. 广播此次commit。还是给各个节点发送 pb.MsgApp 消息，这时候发送给1节点的index变成了index-1=6,commit也变成了6（如果2节点这时候没返回commit，index会使5，commit会是6）
##### 第四步：从节点（1）再次接受到主节点send来的pb.MsgApp消息
1. 还是之前那个流程，唯一改变的点是 commitTo(min(committed,lastnewi)) 的时候，committed和lastnewi都是6，这时候从节点真正的commit了
2. 然后给leader再次发送一个pb.MsgAppResp消息。index还是6（跟上一次没变化）
##### 第五步：主节点再次接收到从节点发送来的 pb.MsgAppResp
1. 调用progress.MaybeUpdate函数返回false，直接退出stepLeader。

一次put流程结束