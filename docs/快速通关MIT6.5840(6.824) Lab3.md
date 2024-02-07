# 快速通关MIT6.5840(6.824) Lab3

最近也是马上要过年了，尤其是做的时候还有小年，也是抽空把lab3A和lab3B给做完了

GitHub仓库地址：[skywircL/MIT6.824: MIT6.5840(MIT6.824)-Spring 2023 (github.com)](https://github.com/skywircL/MIT6.824/)

先上结果：

保证Lab3每个测试都单独测试了100次以上，无一 fail。

不保证绝对的 bug-free

![](https://cdn.jsdelivr.net/gh/skywircL/homework.img/%E6%88%AA%E5%B1%8F2024-02-07%20%E4%B8%8B%E5%8D%884.55.53.png)

![](https://cdn.jsdelivr.net/gh/skywircL/homework.img/%E6%88%AA%E5%B1%8F2024-02-06%2022.55.25.png)

测试时的-p同时并发测试的参数不建议调太大，因为Lab3A中间有几个测试压力还是比较大的，风扇呜呜转如果调的太大时话



## 开始前的准备

阅读论文第八部分：[In Search of an Understandable Consensus Algorithm (mit.edu)](http://nil.csail.mit.edu/6.5840/2023/papers/raft-extended.pdf) 

推荐阅读：[raft_diagram.pdf (mit.edu)](http://nil.csail.mit.edu/6.5840/2023/notes/raft_diagram.pdf)

最后将lab3的文档和代码中各个函数的介绍看完



在对试验的大体代码构建有一定的了解后，就可以开始你的试验了。

## Lab3A & Lab3B

个人共用时：4天

这次的lab可以说是比较简单的了，相较于Lab2和Lab4来说。难度大体和lab1相当，并且lab的代码已经提供了一些基本的函数要你实现，相对而言会比lab1更加有方向一点



### Lab3A

这里要实现论文第八部分提到的超时机制，即如果超时了就换一个server的机制

```go
case <-time.After(100 * time.Millisecond):
		DPrintf("Get Timeout\n")
		reply.Err = ErrTimeOut
```

这样实现即可。`ErrTimeOut`的err状态是我加的，原本只有三个状态，但事实上加不加都没什么关系 ，因为无论是超时还是`ErrNoLeader`，client端都是换一个server重试，他们两个做的事情其实是一样的。

值得一提的是，在论文中换leader是要通过server返回的被其记忆的leaderId，下一次就访问该leader，以及如果超时要重新随机一个leaderId进行访问，但需要修改raft代码（follower要保存一个当前leaderId）以及server代码（reply要加一个返回字段）。因为server的实际数量比较少，并且rpc的reply处也没有说`you have to do`,所以我也懒得去改代码了，偷了个懒，不采用随机，而是采用递增加取余的方式来找leader



在Lab3A的速度测试中，他对你的性能是有要求的：

> Check that ops are committed fast enough, better than 1 per heartbeat interval

如果你只是通过定时的心跳来同步和提交日志，你会发现你通过这个测试的时间和你的心跳时间是有关联的，你心跳越快，他通过需要的时间也越短，反之亦然。

解决方法是在start的时候就立即发送心跳进行同步，而不是等待定时心跳同步

```go
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.isleader

	if isLeader {
		en := Entries{Term: term, Command: command, Index: len(rf.log) + rf.lastIncludedIndex + 1}
		rf.log = append(rf.log, en)
		rf.nextIndex[rf.me] = len(rf.log) + rf.lastIncludedIndex + 1
		rf.persist()
		DPrintf("[start]:me: %d,log:%v\n", rf.me, rf.log)
	} else {
		return -1, term, isLeader
	}
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	reply := &AppendEntriesReply{}
	go rf.sendAppendEntries(args, reply)

	return rf.GetAllLogLen(), term, isLeader
}
```



最后在lab3A中你可能会遇到的情况可能是`history is not linearizable`

一般会生成一个html让你可以可视化地看看是在什么地方出现了线性一致性问题

在mac下的文件生成在`var/folders/9v/l4gt2d_166l5tbz_6f78157r0000gn/T/1581768404.html`

但是我们在实验中是采用了最简单的方法实现线性一致--即通过将所有的操作都提交日志去取得大多数的共识，这在性能上是有损耗的，但是确实能解决线性一致性问题。

所以你会出错大概率可能是因为你把get请求也去重了。



### Lab3B

Lab3B要求实现快照，并且他的测试条件确实会比lab2D要严格些，但是基本没有大的改动，都是对raft的日志压缩部分进行小修小补。

如果你在测试中出现了`logs were not trimmed'`,那大概率是因为你的server端出现了死锁，仔细检查下你实现的代码，应该过lab3b是轻轻松松的。



## 总结

花了4天时间完成lab3，其中在做lab3b的时候我也出现了logs were not trimmed，当时以为是性能问题，一直改来改去就是有两个测试爆这个错过不去，后面仔细分析一下才发现自己死锁了，写着写着给我写迷糊了属于是。正常的完成时间应该是2-3天吧个人觉得，也是比较简单这次试验。

对于lab4可能就先放放了，难度至少等于lab2，花费的时间得7天起步吧，但是寒假所剩不多，还有很多事情没做完，我自然也没有什么时间分配给他它了，大致估计应该是明年初补上lab4和2个challenge吧。maybe









