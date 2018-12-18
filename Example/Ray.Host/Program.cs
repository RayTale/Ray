using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Ray.Core;
using Ray.Core.Client;
using Ray.Core.Messaging;
using Ray.Grain;
using Ray.Handler;
using Ray.IGrains;
using Ray.IGrains.Actors;
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
                //using (var client = await StartClientWithRetries())
                //{
                    //var handlerStartup = client.ServiceProvider.GetService<HandlerStartup>();
                    //await Task.WhenAll(
                    // handlerStartup.Start(SubscriberGroup.Core),
                    // handlerStartup.Start(SubscriberGroup.Db),
                    // handlerStartup.Start(SubscriberGroup.Rep));
                    while (true)
                    {
                        Console.WriteLine("Input any key to stop");
                        Console.ReadLine();
                        //client.Dispose();
                        await host.StopAsync();
                        Console.WriteLine("Input any key to Start");
                        Console.ReadLine();
                        await host.StartAsync();
                    }
                //}
                return 0;
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
                    logging.SetMinimumLevel(LogLevel.Debug);
                    logging.AddConsole();
                });

            var host = builder.Build();
            await host.StartAsync();
            return host;
        }
        private static async Task<IClusterClient> StartClientWithRetries(int initializeAttemptsBeforeFailing = 5)
        {
            int attempt = 0;
            IClusterClient client;
            while (true)
            {
                try
                {
                    client = await ClientFactory.Build(() =>
                    {
                        var builder = new ClientBuilder()
                        .UseLocalhostClustering()
                        .ConfigureServices((context, servicecollection) =>
                        {
                            servicecollection.AddRay();
                            servicecollection.AddSingleton<ISerializer, ProtobufSerializer>();//注册序列化组件
                            servicecollection.AddRabbitMQ();//注册RabbitMq为默认消息队列
                            servicecollection.AddLogging(logging => logging.AddConsole());
                            servicecollection.AddMQHandler();//注册所有handler
                            servicecollection.PostConfigure<RabbitConfig>(c =>
                            {
                                c.UserName = "admin";
                                c.Password = "admin";
                                c.Hosts = new[] { "127.0.0.1:5672" };
                                c.MaxPoolSize = 100;
                                c.VirtualHost = "/";
                            });
                        })
                         .Configure<GrainCollectionOptions>(options =>
                         {
                             options.CollectionAge = TimeSpan.FromMinutes(5);
                         })
                        .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(IAccount).Assembly).WithReferences())
                        .ConfigureLogging(logging => logging.AddConsole());
                        return builder;
                    });
                    Console.WriteLine("Client successfully connect to silo host");
                    break;
                }
                catch (SiloUnavailableException)
                {
                    attempt++;
                    Console.WriteLine($"Attempt {attempt} of {initializeAttemptsBeforeFailing} failed to initialize the Orleans client.");
                    if (attempt > initializeAttemptsBeforeFailing)
                    {
                        throw;
                    }
                    await Task.Delay(TimeSpan.FromSeconds(4));
                }
            }

            return client;
        }
    }
}
