using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.TestingHost;
using Ray.Core;
using Ray.Core.Services;
using Ray.EventBus.RabbitMQ;
using Ray.Storage.PostgreSQL;
using RayTest.Grains;
using RayTest.Grains.Grains;
using RayTest.IGrains;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Xunit;

namespace RayCore.Tests
{
    public class TransferTest
    {
        readonly TestCluster cluster;
        public TransferTest()
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
        public async Task TopUp()
        {
            var accountActor = cluster.Client.GetGrain<IAccount>(1);
            var balance = await accountActor.GetBalance();
            await Task.WhenAll(Enumerable.Range(0, 100).Select(x => accountActor.TopUp(100)));
            var newBalance = await accountActor.GetBalance();
            Assert.Equal(balance + 100 * 100, newBalance);
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
            .AddRay<TransferConfig>()
            .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback)
            .ConfigureApplicationParts(parts =>
            {
                parts.AddApplicationPart(typeof(Account).Assembly).WithReferences();
                parts.AddApplicationPart(typeof(UtcUIDGrain).Assembly).WithReferences();
            })
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
                servicecollection.AddRabbitMQ(config =>
                {
                    config.UserName = "guest";
                    config.Password = "guest";
                    config.Hosts = new[] { "127.0.0.1:5672" };
                    config.VirtualHost = "/";
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
