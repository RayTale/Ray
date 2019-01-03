using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Ray.Core;
using Ray.Core.Abstractions;
using Ray.Core.Client;
using Ray.Grain;
using Ray.Handler;
using Ray.IGrains;
using Ray.MongoDB;
using Ray.PostgreSQL;
using Ray.RabbitMQ;

namespace Ray.MongoHost
{
    class Program
    {
        static int Main(string[] args)
        {
            return RunMainAsync().Result;
        }
        private static async Task<int> RunMainAsync()
        {
            try
            {
                using (var host = await StartSilo())
                {
                    //var handlerStartup = host.Services.GetService<HandlerStartup>();
                    //await Task.WhenAll(
                    // handlerStartup.Start(SubscriberGroup.Core),
                    // handlerStartup.Start(SubscriberGroup.Db),
                    // handlerStartup.Start(SubscriberGroup.Rep));
                    while (true)
                    {
                        Console.WriteLine("Input any key to stop");
                        Console.ReadLine();
                        await host.StopAsync();
                        Console.WriteLine("Input any key to Start");
                        Console.ReadLine();
                        await host.StartAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return 1;
            }
        }
        private static async Task<ISiloHost> StartSilo()
        {
            var builder = new SiloHostBuilder()
                .UseLocalhostClustering()
                .UseDashboard()
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback)
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(Account).Assembly).WithReferences())
                .ConfigureServices((context, servicecollection) =>
                {
                    servicecollection.AddRay();
                    servicecollection.AddSingleton<ISerializer, ProtobufSerializer>();//注册序列化组件
                    //注册postgresql为事件存储库
                    servicecollection.AddPSqlSiloGrain();
                    //注册mongodb为事件存储库
                    //servicecollection.AddMongoDbSiloGrain();
                    servicecollection.AddMQHandler();//注册所有handler
                    servicecollection.AddSingleton<IClientFactory, ClientFactory>();
                })
                 .Configure<GrainCollectionOptions>(options =>
                 {
                     options.CollectionAge = TimeSpan.FromMinutes(5);
                 })
                .Configure<SqlConfig>(c =>
                {
                    c.ConnectionDict = new Dictionary<string, string> {
                        { "core_event","Server=127.0.0.1;Port=5432;Database=Ray;User Id=postgres;Password=extop;Pooling=true;MaxPoolSize=20;"}
                    };
                })
                .Configure<MongoConfig>(c =>
                {
                    c.Connection = "mongodb://127.0.0.1:27017";
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
                    logging.SetMinimumLevel(LogLevel.Information);
                    logging.AddConsole(options => options.IncludeScopes = true);
                }).EnableDirectClient();

            var host = builder.Build();
            await host.StartAsync();
            return host;
        }
    }
    public class ClientFactory : IClientFactory
    {
        readonly IServiceProvider serviceProvider;
        public ClientFactory(IServiceProvider serviceProvider)
        {
            this.serviceProvider = serviceProvider;
        }
        public IClusterClient Create()
        {
            return serviceProvider.GetService<IClusterClient>();
        }
    }
}
