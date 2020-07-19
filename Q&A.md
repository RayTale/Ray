1. Orleans与Akka对比，为什么选用Orleans？

    答: Akka对参与开发的人员要求更高一些，普遍是专家级别，Orleans框架进一步抽象了一层，结合C#语言特性，能普遍降低开发难度。
    
    下面是知乎网友的答案，可以参考：
    
    ![image](https://note.youdao.com/yws/api/personal/file/71CBAC03AB024F86A3C6A204257C706A?method=download&shareKey=d05b6c5a943db4967fee036c8639045f)
    
    原文地址：https://www.zhihu.com/question/31472959
    
    扩展阅读：http://www.cnblogs.com/xuezy/articles/5621764.html

2. 文中提到Ray"无数据库事务"，但https://github.com/RayTale/Ray中有代码：

    ```
    servicecollection.AddMongoES();//注册MongoDB为事件库
    servicecollection.AddRabbitMQ<MessageInfo>();//注册RabbitMq为默认消息队列
    ```
    
    是否需要MogoDB作为数据库？RabbitMQ作为消息存储队列？
    
    答：目前Ray支持MogoDB和PostgreSQL持久化存储事件。是的，RabbitMQ作为消息存储队列，确切点是用来存储传递中的事件，或者说通过RabbitMQ传递事件。

3. 您好，在看Ray框架还有您的一篇文章，想问下Service Fabric在你给的架构图中是做什么用的，Service Fabric 没有怎么了解？是不是我不用也可以的，只是使用 Asp.Net Core 开发API 调用 Ray 层就可以了？

    答：
    - 在项目中，我们的项目都部署在SF上。最后那个架构图是我们公司3.0系统的技术堆栈，想给大家了解一下Ray的使用位置。
    - ServiceFabric不用也可以。Ray是一个Actor框架，Actor一般做服务层，客户端和宿主根据需求可以是控制台，WinForm，Asp.Net MVC,可以只使用 Asp.Net Core 开发API调用Actor的服务层。
    - 补充：ServiceFabric 中也有Actor，感兴趣可以了解一下。

4. Orleans与ServiceFabric Actor的对比。

    ![image](https://note.youdao.com/yws/api/personal/file/7534E644DF5C482992C75E3FCB832E28?method=download&shareKey=4c69d7d87c53a9e7ac41b4426af81a9b)
    
    原文地址：http://richorama.github.io/2016/07/08/orleans-vs-service-fabric/
