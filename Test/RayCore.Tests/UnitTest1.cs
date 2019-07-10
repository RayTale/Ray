using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using Ray.Core;
using Ray.EventBus.RabbitMQ;
using Ray.Storage.PostgreSQL;
using RayTest.Grains;
using RayTest.Grains.Account;
using RayTest.IGrains.Actors;
using Xunit;

namespace RayCore.Tests
{
    public class UnitTest1
    {
        readonly TestCluster cluster;
        public UnitTest1()
        {
            var build = new TestClusterBuilder();
            build.AddSiloBuilderConfigurator<TestSiloConfigurator>();
            build.AddClientBuilderConfigurator<TestClientConfigurator>();
            cluster = build.Build();
            if (cluster?.Primary == null)
            {
                cluster?.Deploy();
            }
        }
        [Fact]
        public async Task Test1()
        {
            var accountActor = cluster.Client.GetGrain<IAccount>(1);
            var balance = await accountActor.GetBalance();
            await accountActor.AddAmount(100);
            var newBalance = await accountActor.GetBalance();
            Assert.Equal(balance + 100, newBalance);
        }
    }
    public class TestClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder
            .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(IAccount).Assembly).WithReferences())
            .ConfigureLogging(logging => logging.AddConsole());
        }
    }
    public class TestSiloConfigurator : ISiloBuilderConfigurator
    {
        public void Configure(ISiloHostBuilder hostBuilder)
        {
            hostBuilder
            .AddRay<Configuration>()
            .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(Account).Assembly).WithReferences())
            .ConfigureServices((context, servicecollection) =>
            {
                //×¢²ápostgresqlÎªÊÂ¼þ´æ´¢¿â
                servicecollection.AddPostgreSQLStorage(config =>
                {
                    config.ConnectionDict = new Dictionary<string, string>
                        {
                            { "core_event","Server=127.0.0.1;Port=5432;Database=Ray_Test;User Id=postgres;Password=extop;Pooling=true;MaxPoolSize=20;"}
                        };
                });
                servicecollection.PSQLConfigure();
                servicecollection.AddRabbitMQ(config =>
                {
                    config.UserName = "admin";
                    config.Password = "admin";
                    config.Hosts = new[] { "127.0.0.1:5672" };
                    config.MaxPoolSize = 100;
                    config.VirtualHost = "/";
                }, async container =>
                {
                    await container.CreateEventBus<Account>("Account", "account", 5).DefaultConsumer<long>();
                });
            })
            .ConfigureLogging(logging =>
            {
                logging.SetMinimumLevel(LogLevel.Error);
                logging.AddConsole();
            }); ;
        }
    }
}
