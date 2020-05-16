using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Ray.Core;
using Ray.Core.Services;
using Ray.EventBus.RabbitMQ;
using Ray.Storage.PostgreSQL;
using System;
using System.Net;
using System.Threading.Tasks;
using Transfer.Grains;
using Transfer.Grains.Grains;

namespace Transfer.Server
{
    class Program
    {
        static Task Main(string[] args)
        {
            var host = CreateHost();
            return host.RunAsync();
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
                    //注册postgresql为事件存储库
                    servicecollection.AddPostgreSQLStorage(config =>
                    {
                        config.ConnectionDict.Add("core_event", "Server=192.168.1.10;Port=5432;Database=Ray;User Id=postgres;Password=postgres;Pooling=true;MaxPoolSize=20;");
                    });
                    //servicecollection.AddKafkaMQ(
                    //config => { },
                    //config =>
                    //{
                    //    config.BootstrapServers = "192.168.1.2:9092";
                    //}, config =>
                    //{
                    //    config.BootstrapServers = "192.168.1.2:9092";
                    //});
                    servicecollection.AddRabbitMQ(options =>
                    {
                        options.VirtualHost = "/";
                        options.Hosts = new string[] { "192.168.1.10:5672" };
                        options.UserName = "guest";
                        options.Password = "guest";
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
