using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using Ray.Core;
using Ray.Core.Serialization;
using Ray.EventBus.RabbitMQ;
using Ray.Storage.PostgreSQL;
using RayTest.Grains;
using RayTest.IGrains;
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
                .AddRay<MessageInfo>()
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(Account).Assembly).WithReferences())
                .ConfigureServices((context, servicecollection) =>
                {
                    servicecollection.AddSingleton<ISerializer, ProtobufSerializer>();
                    servicecollection.AddPSqlSiloGrain();
                })
                .Configure<SqlConfig>(c =>
                {
                    c.ConnectionDict = new Dictionary<string, string> {
                             { "core_event","Server=127.0.0.1;Port=5432;Database=Ray;User Id=postgres;Password=extop;Pooling=true;MaxPoolSize=50;Timeout=10;"}
                    };
                })
                .Configure<RabbitConfig>(c =>
                {
                    c.UserName = "admin";
                    c.Password = "admin";
                    c.Hosts = new[] { "127.0.0.1:5672" };
                    c.MaxPoolSize = 100;
                    c.VirtualHost = "/";
                })
                .ConfigureLogging(logging =>
                {
                    logging.SetMinimumLevel(LogLevel.Error);
                    logging.AddConsole();
                }); ;
        }
    }
}
