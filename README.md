# Ray
这是一个集成Actor,Event Sourcing(事件溯源),Eventual consistency(最终一致性)的无数据库事务，高性能分布式云框架(构建集群请参阅:http://dotnet.github.io/orleans/) 

### 案例启动步骤

案例里是一个简单的无事务转账功能。还有很多给力的功能，后面会持续放出文档

一、安装mongodb,rabbitmq.

二、修改Host代码中的mongodb和rabbitmq的配置信息.

```csharp
    var builder = new SiloHostBuilder()
        .UseConfiguration(config)
        .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(Account).Assembly).WithReferences())
        .ConfigureServices((context, servicecollection) =>
        {
            servicecollection.AddSingleton<ISerializer, ProtobufSerializer>();//注册序列化组件
            servicecollection.AddMongoES();//注册MongoDB为事件库
            servicecollection.AddRabbitMQ<MessageInfo>();//注册RabbitMq为默认消息队列
        })
        .Configure<MongoConfig>(c => c.Connection = "mongodb://127.0.0.1:28888")
        .Configure<RabbitConfig>(c =>
        {
            c.UserName = "admin";
            c.Password = "luohuazhiyu";
            c.Hosts = new[] { "127.0.0.1:5672" };
            c.MaxPoolSize = 100;
            c.VirtualHost = "/";
        })
        .ConfigureLogging(logging => logging.AddConsole());
```

三、修改client代码中的rabbitmq的配置信息.

```csharp
    var config = ClientConfiguration.LocalhostSilo();
    var client = new ClientBuilder()
        .UseConfiguration(config)
        .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(IAccount).Assembly).WithReferences())
        .ConfigureLogging(logging => logging.AddConsole())
        .ConfigureServices((servicecollection) =>
        {
            servicecollection.AddSingleton<ISerializer, ProtobufSerializer>();//注册序列化组件
            servicecollection.AddRabbitMQ<MessageInfo>();//注册RabbitMq为默认消息队列
            servicecollection.PostConfigure<RabbitConfig>(c =>
            {
                c.UserName = "admin";
                c.Password = "luohuazhiyu";
                c.Hosts = new[] { "127.0.0.1:5672" };
                c.MaxPoolSize = 100;
                c.VirtualHost = "/";
            });
        })
        .Build();
```
四、启动Ray.Host

五、启动Ray.Client

---

Note:

Docker开发环境:
使用Docker可以方便搭建测试环境，调整对应参数，运行示例
```
docker run -d -p 5672:5672 -p 15672:15672 --hostname rabbit --name rabbit -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin  rabbitmq:3-management
docker run -d -p 27017:27017 --name mongo -td mongo 
docker run -d -p 5432:5432 --name postgres -e POSTGRES_PASSWORD=123456  postgres
```
