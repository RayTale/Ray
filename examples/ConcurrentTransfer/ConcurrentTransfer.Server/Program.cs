using System;
using System.Net;
using System.Threading.Tasks;
using ConcurrentTransfer.Grains;
using ConcurrentTransfer.Grains.Grains;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Ray.Core;
using Ray.Core.Services;
using Ray.EventBus.RabbitMQ;
using Ray.Metric.Core;
using Ray.Metric.Core.Actors;
using Ray.Metric.Prometheus;
using Ray.Storage.PostgreSQL;

namespace ConcurrentTransfer.Server
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
                        .AddRayMetric()
                        .MetricPushToPrometheus(options =>
                        {
                            options.ServiceName = "concurrent-transfer";
                            options.PushEndpoint = "http://localhost:9091";
                        })
                        .ConfigureApplicationParts(parts =>
                        {
                            parts.AddApplicationPart(typeof(MonitorActor).Assembly).WithReferences();
                            parts.AddApplicationPart(typeof(Account).Assembly).WithReferences();
                            parts.AddApplicationPart(typeof(UtcUIDGrain).Assembly).WithReferences();
                        });
                })
                .ConfigureServices(servicecollection =>
                {
                    //Register postgresql as an event repository
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
                        options.Hosts = new string[] { "localhost:5672" };
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
