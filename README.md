# Ray

[![Join the chat at https://gitter.im/RayTale/Ray](https://badges.gitter.im/RayTale/Ray.svg)](https://gitter.im/RayTale/Ray?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This is a high-performance distributed framework that integrates Actor, Event Sourcing, and Eventual consistency (see: http://dotnet.github.io/orleans/)

### Case start steps

In the case is a simple transactionless transfer function

1. Install (mongodb or postgresql or mysql or sqlserver) and (rabbitmq or kafka).

2. Select the event persistence method and EventBus in Program.cs of the Ray.Host project.

```csharp
    var builder = new SiloHostBuilder()
        .UseConfiguration(config)
        .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(Account).Assembly).WithReferences())
        .ConfigureServices((context, servicecollection) =>
        {
            //Register postgresql as an event repository
            servicecollection.AddPostgreSQLStorage(config =>
            {
                config.ConnectionDict.Add("core_event", "Server=127.0.0.1;Port=5432;Database=Ray;User Id=postgres;Password=XXXX;Pooling=true;MaxPoolSize=20;");
            });
            //Configure distributed transaction manager (not required, only need to be set if distributed transaction is required)
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
            c.Hosts = new[] {"127.0.0.1:5672" };
            c.MaxPoolSize = 100;
            c.VirtualHost = "/";
        })
        .ConfigureLogging(logging => logging.AddConsole());
```

Third, modify the configuration information of Ray.Client.

```csharp
    var config = ClientConfiguration.LocalhostSilo();
    var client = new ClientBuilder()
        .UseConfiguration(config)
        .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(IAccount).Assembly).WithReferences())
        .ConfigureLogging(logging => logging.AddConsole())
        .Build();
    await client.Connect();
```
Fourth, start Ray.Host

Five, start Ray.Client

```
