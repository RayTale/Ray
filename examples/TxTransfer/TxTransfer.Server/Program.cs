using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Ray.Core;
using Ray.Core.Services;
using Ray.EventBus.Kafka;
using Ray.Storage.PostgreSQL;
using TxTransfer.Grains;
using TxTransfer.Grains.Grains;

namespace TxTransfer.Server
{
    class Program
    {
        static async Task Main()
        {
            var host = CreateHost();
            await host.RunAsync();
        }
        private static IHost CreateHost()
        {
            return new HostBuilder()
                .UseOrleans((context, siloBuilder) =>
                {
                    siloBuilder
                        .UseLocalhostClustering()
                        .UseDashboard()
                        .AddRay<TransferConfig>()
                        .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback)
                        .ConfigureApplicationParts(parts =>
                        {
                            parts.AddApplicationPart(typeof(Account).Assembly).WithReferences();
                            parts.AddApplicationPart(typeof(UtcUIDGrain).Assembly).WithReferences();
                        });
                })
                .ConfigureServices(servicecollection =>
                {
                    //Register postgresql as an event repository
                    servicecollection.AddPostgreSQLStorage(config =>
                {
                    config.ConnectionDict.Add("core_event", "Server=127.0.0.1;Port=5432;Database=Ray;User Id=postgres;Password=luohuazhiyu;Pooling=true;MaxPoolSize=20;");
                    });
                    servicecollection.AddPostgreSQLTxStorage(options =>
                    {
                        options.ConnectionKey = "core_event";
                        options.TableName = "Transaction_Record";
                    });
                    servicecollection.AddKafkaMQ(
                    config => { },
                    config =>
                    {
                        config.BootstrapServers = "192.168.1.3:9092";
                    }, config =>
                    {
                        config.BootstrapServers = "192.168.1.3:9092";
                    });
                    servicecollection.Configure<GrainCollectionOptions>(options =>
                    {
                        options.CollectionAge = TimeSpan.FromMinutes(5);
                    });
                })
                .ConfigureLogging(logging =>
                {
                    logging.SetMinimumLevel(LogLevel.Information);
                    logging.AddConsole(options => options.IncludeScopes = true);
                }).Build();
        }
    }
}
