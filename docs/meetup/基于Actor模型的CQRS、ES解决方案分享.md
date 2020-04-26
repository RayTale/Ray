##### 开场白

大家晚上好，今天跟大家分享的话题是《基于Actor模型的CQRS/ES解决方案分享》，最近一段时间我一直是这个话题的学习者、追随者，这个话题目前生产环境落地的资料少一些，分享的内容中有一些我个人的思考和理解，如果分享的内容有误、有疑问欢迎大家提出，希望通过分享这种沟通方式大家相互促进，共同进步。

##### 引言

1. 话题由三部分组成：

- Actor模型&Orleans：在编程的层面，从细粒度-由下向上的角度介绍Actor模型；
- CQRS/ES：在框架的层面，从粗粒度-由上向下的角度介绍Actor模型，说明Orleans技术在架构方面的价值；
- Service Fabric：从架构部署的角度将上述方案落地上线。
2. 群里的小伙伴技术栈可能多是Java和Go体系，分享的话题主要是C#技术栈，没有语言纷争，彼此相互学习。比如：Scala中，Actor模型框架有akka，CQRS/ES模式与编程语言无关，Service Fabric与K8S是同类平台，可以相互替代，我自己也在学习K8S。

##### Actor模型&Orleans（细粒度）

1. 共享内存模型

多核处理器出现后，大家常用的并发编程模型是共享内存模型。

![共享内存模型图](https://note.youdao.com/yws/api/personal/file/6E3C1F8D0A22425287A98D6C1A90B2DB?method=download&shareKey=e76c0bb4ebf17695522dbccc857ec454)

这种编程模型的使用带来了许多痛点，比如：

- 编程：多线程、锁、并发集合、异步、设计模式（队列、约定顺序、权重）、编译
- 无力：单系统的无力性：①地理分布型②容错型
- 性能：锁，性能会降低
- 测试：
    - 从坑里爬出来不难，难的是我们不知道自己是不是在坑里（开发调试的时候没有热点可能是正常的）
    - 遇到bug难以重现。有些问题特别是系统规模大了，可能运行几个月才能重现问题
- 维护：
    - 我们要保证所有对象的同步都是正确的、顺序的获取多个锁。
    - 12个月后换了另外10个程序员仍然按照这个规则维护代码。

简单总结：
- 并发问题确实存在
- 共享内存模型正确使用掌握的知识量多
- 加锁效率就低
- 存在许多不确定性

2. Actor模型

Actor模型是一个概念模型，用于处理并发计算。Actor由3部分组成：状态（State）+行为（Behavior）+邮箱（Mailbox），State是指actor对象的变量信息，存在于actor之中，actor之间不共享内存数据，actor只会在接收到消息后，调用自己的方法改变自己的state，从而避免并发条件下的死锁等问题；Behavior是指actor的计算行为逻辑；邮箱建立actor之间的联系，一个actor发送消息后，接收消息的actor将消息放入邮箱中等待处理，邮箱内部通过队列实现，消息传递通过异步方式进行。

![image](https://note.youdao.com/yws/api/personal/file/0B6CA741F9BC479B8F355E086BDE3026?method=download&shareKey=64ea0b49098f1d0a1bb4e5119d27a8a3)

Actor是分布式存在的内存状态及单线程计算单元，一个Id对应的Actor只会在集群种存在一个（有状态的 Actor在集群中一个Id只会存在一个实例，无状态的可配置为根据流量存在多个）,使用者只需要通过Id就能随时访问不需要关注该Actor在集群的什么位置。单线程计算单元保证了消息的顺序到达,不存在Actor内部状态竞用问题。

举个例子：

多个玩家合作在打Boss，每个玩家都是一个单独的线程，但是Boss的血量需要在多个玩家之间同步。同时这个Boss在多个服务器中都存在，因此每个服务器都有多个玩家会同时打这个服务器里面的Boss。

如果多线程并发请求，默认情况下它只会并发处理。这种情况下可能造成数据冲突。但是Actor是单线程模型，意味着即使多线程来通过Actor ID调用同一个Actor，任何函数调用都是只允许一个线程进行操作。并且同时只能有一个线程在使用一个Actor实例。

3. Actor模型：Orleans

Actor模型这么好，怎么实现？

可以通过特定的Actor工具或直接使用编程语言实现Actor模型，Erlang语言含有Actor元素，Scala可以通过Akka框架实现Actor编程。C#语言中有两类比较流行，Akka.NET框架和Orleans框架。这次分享内容使用了Orleans框架。

特点：

Erlang和Akka的Actor平台仍然使开发人员负担许多分布式系统的复杂性：关键的挑战是开发管理Actor生命周期的代码，处理分布式竞争、处理故障和恢复Actor以及分布式资源管理等等都很复杂。Orleans简化了许多复杂性。

优点：
- 降低开发、测试、维护的难度
- 特殊场景下锁依旧会用到，但频率大大降低，业务代码里甚至不会用到锁
- 关注并发时，只需要关注多个actor之间的消息流
- 方便测试
- 容错
- 分布式内存

缺点：
- 也会出现死锁（调用顺序原因）
- 多个actor不共享状态，通过消息传递，每次调用都是一次网络请求，不太适合实施细粒度的并行
- 编程思维需要转变

![image](https://note.youdao.com/yws/api/personal/file/B4BE32EE96C141329FFBE996410CF5B2?method=download&shareKey=2e1375ef61efafa2b35ddb4da655f3bf)

---

第一小节总结：上面内容由下往上，从代码层面细粒度层面表达了采用Actor模型的好处或原因。

---

##### CQRS/ES（架构层面）

1. 从1000万用户并发修改用户资料的假设场景开始

![image](https://note.youdao.com/yws/api/personal/file/5966C19B7ACE41A899B744343E74ECE8?method=download&shareKey=45237c8dbbef3d6225449afa5b7469ba)

1. 每次修改操作耗时200ms，每秒5个操作
2. MySQL连接数在5K，分10个库
3. 5 *5k *10=25万TPS
4. 1000万/25万=40s

![image](https://note.youdao.com/yws/api/personal/file/117044BEDA3446149BFBE24C4650015D?method=download&shareKey=34d1795eb806ec33b40b95d127bb30a1)

在秒杀场景中，由于对乐观锁/悲观锁的使用，推测系统响应时间更复杂。

2. 使用Actor解决高并发的性能问题

![image](https://note.youdao.com/yws/api/personal/file/A46B68314102409CA50BA079771B0DC2?method=download&shareKey=5e581d5a203f6ff983b7d3506fbec804)

1000万用户，一个用户一个Actor，1000万个内存对象。

![image](https://note.youdao.com/yws/api/personal/file/11037DE5572541CFBEEEA11FDF805CDB?method=download&shareKey=612fcede38c92967822682edc93623c6)

200万件SKU，一件SKU一个Actor，200万个内存对象。

- 平均一个SKU承担1000万/200万=5个请求
- 1000万对数据库的读写压力变成了200万
- 1000万的读写是同步的，200万的数据库压力是异步的
- 异步落盘时可以采用批量操作

总结：

由于1000万+用户的请求根据购物意愿分散到200万个商品SKU上：
每个内存领域对象都强制串行执行用户请求，避免了竞争争抢；
内存领域对象上扣库存操作处理时间极快，基本没可能出现请求阻塞情况；

从架构层面彻底解决高并发争抢的性能问题。
理论模型，TPS>100万+……

3. EventSourcing：内存对象高可用保障

Actor是分布式存在的内存状态及单线程计算单元，采用EventSourcing只记录状态变化引发的事件，事件落盘时只有Add操作，上述设计中很依赖Actor中State，事件溯源提高性能的同时，可以用来保证内存数据的高可用。

![image](https://note.youdao.com/yws/api/personal/file/0D5F50C5333547ABBA334B6D5166DF5A?method=download&shareKey=6f409f467c73f2df357343f82b91dc37)

![image](https://note.youdao.com/yws/api/personal/file/A77C25B5E9314293A54AB37F702D0ABF?method=download&shareKey=80c784e94a3249dc9e2b6f2641b16370)

4. CQRS

上面1000万并发场景的内容来自网友分享的PPT，与我们实际项目思路一致，就拿来与大家分享这个过程，下图是我们交易所项目中的架构图：

![image](https://note.youdao.com/yws/api/personal/file/4BC3080DF87B49D4976D156F91B9A734?method=download&shareKey=eec79b78ea3d87e0031c7804e0f0b78d)

开源版本架构图：

![image](https://note.youdao.com/yws/api/personal/file/B8A0DBAA75194CD08943F8B70DE6A8D2?method=download&shareKey=eb10b26e4af1c3ff0f3da35fa86b431a)

（开源项目github：https://github.com/RayTale/Ray）

---

第二小节总结：由上往下，架构层面粗粒度层面表达了采用Actor模型的好处或原因。

---

##### Service Fabric 

系统开发完成后Actor要组成集群，系统在集群中部署，实现高性能、高可用、可伸缩的要求。部署阶段可以选择Service Fabric或者K8S，目的是降低分布式系统部署、管理的难度，同时满足弹性伸缩。

交易所项目可以采用Service Fabric部署，也可以采用K8S，当时K8S还没这么流行，我们采用了Service Fabric，Service Fabric 是一款微软开源的分布式系统平台，可方便用户轻松打包、部署和管理可缩放的可靠微服务和容器。开发人员和管理员不需解决复杂的基础结构问题，只需专注于实现苛刻的任务关键型工作负荷，即那些可缩放、可靠且易于管理的工作负荷。支持Windows与Linux部署，Windows上的部署文档齐全，但在Linux上官方资料没有。现在推荐K8S。

---

第三小节总结：
1. 借助Service Fabric或K8S实现低成本运维、构建集群的目的。
2. 建立分布式系统的两种最佳实践：
- 进程级别：容器+运维工具（k8s/sf）
- 线程级别：Actor+运维工具（k8s/sf）

---

上面是我对今天话题的分享。

参考：
1. ES/CQRS部分内容参考：《领域模型 + 内存计算 + 微服务的协奏曲：乾坤（演讲稿）》
2017年互联网应用架构实战峰会
2. 其他细节来自互联网，不一一列出

---

##### 讨论

汤雪华:
1000W用户，购买200W SKU，如果不考虑热点SKU，则每个SKU平均为5个并发减库存的更新；
而总共的SKU分10个数据库存储，则每个库存储20W SKU。所以20W * 5 = 100W个并发的减库存；

汤雪华:
每个库负责100W的并发更新，这个并发量，不管是否采用actor/es，都要采用group commit的技术

汤雪华:
否则单机都不可能达到100W/S的数据写入。

汤雪华:
采用es的方式，就是每秒插入100W个事件；不采用ES，就是每秒更新100W次商品减库存的SQL update语句

杨华:
哦

汤雪华:
不过实际上，除了阿里的体量，不可能并发达到1000W的

汤雪华:
1000W用户不代表1000W并发

汤雪华:
如果真的是1000W并发，可能实际在线用户至少有10亿了

汤雪华:
因为如果只有1000W在线用户，那是不可能这些用户同时在同一秒内发起购买的，大家想一下是不是这样

杨华:
这么熟的名字

汤雪华:
所以，1000W在线用户的并发实际只有10W最多了

杨华:
@严永恒 

汤雪华:
也就是单机只有1W的并发更新，不需要group commit也无压力

杨华:
嗯

##### 问答

Q1：单点故障后，正在处理的 cache
数据如何处理的，例如，http,tcp请求…毕竟涉及到钱

A：actor有激活和失活的生命周期，激活的时候使用快照和Events来恢复最新内存状态，失活的时候保存快照。actor框架保证系统中同一个key只会存在同一个actor，当单点故障后，actor会在其它节点重建并恢复最新状态。

Q2：event ID生成的速度如何保证有效的scale？有没有遇到需要后期插入一些event，修正前期系统运行的bug？有没有遇到需要把前期已经定好的event再拆细的情况？有遇到系统错误，需要replay event的情况？
A：1. 当时项目中event ID采用了MongoDB的ObjectId生成算法，没有遇到问题；有遇到后期插入event修正之前bug的情况；有遇到将已定好的event修改的情况，采用的方式是加版本号；没有，遇到过系统重新迁移删除快照重新replay event的情况。

Q3：数据落地得策略是什么？还是说就是直接落地？
A：event数据直接落地；用于支持查询的数据，是Handler消费event后异步落库。

Q4：actor跨物理机器集群事务怎么处理？
A：结合事件溯源，采用最终一致性。

Q5：Grain Persistence使用Relational Storage容量和速度会不会是瓶颈？
A：Grain Persistence存的是Grain的快照和event，event是只增的，速度没有出现瓶颈，而且开源版本测试中PostgreSQL性能优于MongoDB，在存储中针对这两个方面做了优化：比如分表、归档处理、快照处理、批量处理。

Q6：SF中的reliable collection对应到k8s是什么？
A：不好意思，这个我不清楚。

Q7：开发语言是erlang吗？Golang有这样的开发模型库支持吗？
A：开发语言是C#。Golang我了解的不多，proto.actor可以了解一下：https://github.com/AsynkronIT/protoactor-go

Q8：能否来几篇博客阐述如何一步步使用orleans实现一个简单的事件总线
A：事件总线的实现使用的是RabbitMQ，这个可以看一下开源版本的源码EventBus.RabbitMQ部分，博客的可能后面会写，如果不996的话（笑脸）

Q9：每个pod的actor都不一样，如何用k8s部署actor，失败的节点如何监控，并借助k8s自动恢复？
A：actor是无状态的，失败恢复依靠重新激活时事件溯源机制。k8s部署actor官方有支持，可以参考官方示例。在实际项目中使用k8s部署Orleans，我没有实践过，后来有同事验证过可以，具体如何监控不清楚。

Q10：Orleans中，持久化事件时，是否有支持并发冲突的检测，是如何实现的？
A：Orleans不支持；工作中，在事件持久化时做了这方面的工作，方式是根据版本号。

Q11：Orleans中，如何判断消息是否重复处理的？因为分布式环境下，同一个消息可能会被重复发送到actor mailbox中的，而actor本身无法检测消息是否重复过来。
A：是的，在具体项目中，通过框架封装实现了幂等性控制，具体细节是通过插入事件的唯一索引。

Q12：同一个actor是否会存在于集群中的多台机器？如果可能，怎样的场景下可能会出现这种情况？
A：一个Id对应的Actor只会在集群种存在一个。

Q13：
响应式架构 消息模式Actor实现与Scala.Akka应用集成 这本书对理解actor的帮助大吗，还有实现领域驱动设计这本

A：这本书我看过，刚接触这个项目时看的，文章说的有些深奥，因为当时关注的是Orleans，文中讲的是akka，帮助不大，推荐具体项目的官方文档。实现领域驱动这本书有收获，推荐专题式阅读，DDD多在社区交流。