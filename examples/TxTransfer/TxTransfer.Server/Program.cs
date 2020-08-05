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
using Ray.Storage.PostgreSQL;
using TxTransfer.Grains;
using TxTransfer.Grains.Grains;
using Ray.EventBus.RabbitMQ;
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
                .ConfigureServices(serviceCollection =>
                {
                    //Register postgresql as an event repository
                    serviceCollection.AddPostgreSQLStorage(config =>
                {
                    config.ConnectionDict.Add("core_event", "Server=localhost;Port=5432;Database=Ray;User Id=postgress;Password=postgress;Pooling=true;MaxPoolSize=20;");
                    });
                    serviceCollection.AddPostgreSQLTxStorage(options =>
                    {
                        options.ConnectionKey = "core_event";
                        options.TableName = "Transaction_Record";
                    });

                    serviceCollection.AddRabbitMQ(options =>
                    {
                        options.VirtualHost = "/";
                        options.Hosts = new string[] { "localhost:5672" };
                        options.UserName = "guest";
                        options.Password = "guest";
                    });

                    //servicecollection.AddKafkaMQ(
                    //config => { },
                    //config =>
                    //{
                    //    config.BootstrapServers = "192.168.1.3:9092";
                    //}, config =>
                    //{
                    //    config.BootstrapServers = "192.168.1.3:9092";
                    //});
                    serviceCollection.Configure<GrainCollectionOptions>(options =>
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
