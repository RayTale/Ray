# Ray
这是一个集成Actor,Event Sourcing,Eventual consistency的高性能分布式框架(构建分布式集群请参阅:http://dotnet.github.io/orleans/) 

### 案例启动步骤

案例里是一个简单的无事务转账功能

一、安装(mongodb or postgresql or mysql or sqlserver) and (rabbitmq or kafka)。

二、在Ray.Host项目的Program.cs中选择事件持久化方式和EventBus。

```csharp
    var builder = new SiloHostBuilder()
        .UseConfiguration(config)
        .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(Account).Assembly).WithReferences())
        .ConfigureServices((context, servicecollection) =>
        {
            //注册postgresql为事件存储库
            servicecollection.AddPostgreSQLStorage(config =>
            {
                config.ConnectionDict.Add("core_event", "Server=127.0.0.1;Port=5432;Database=Ray;User Id=postgres;Password=XXXX;Pooling=true;MaxPoolSize=20;");
            });
            //配置分布式事务管理器(非必须，需要分布式事务才需设置)
            servicecollection.AddPostgreSQLTxStorage(options =>
            {
                options.ConnectionKey = "core_event";
                options.TableName = "Transaction_TemporaryRecord";
            });
            servicecollection.PSQLConfigure();
        })
        .Configure<MongoConfig>(c => c.Connection = "mongodb://127.0.0.1:28888")
        .Configure<RabbitConfig>(c =>
        {
            c.UserName = "admin";
            c.Password = "XXXX";
            c.Hosts = new[] { "127.0.0.1:5672" };
            c.MaxPoolSize = 100;
            c.VirtualHost = "/";
        })
        .ConfigureLogging(logging => logging.AddConsole());
```

三、修改Ray.Client的配置信息.

```csharp
    var config = ClientConfiguration.LocalhostSilo();
    var client = new ClientBuilder()
        .UseConfiguration(config)
        .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(IAccount).Assembly).WithReferences())
        .ConfigureLogging(logging => logging.AddConsole())
        .Build();
    await client.Connect();
```
四、启动Ray.Host

五、启动Ray.Client

```
