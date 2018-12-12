using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Ray.Core;
using Ray.Core.Client;
using Ray.Core.Messaging;
using Ray.Handler;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.RabbitMQ;

namespace Ray.Client
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
                using (var client = await StartClientWithRetries())
                {
                    var handlerStartup = client.ServiceProvider.GetService<HandlerStartup>();
                    await Task.WhenAll(
                     handlerStartup.Start(SubscriberGroup.Core),
                     handlerStartup.Start(SubscriberGroup.Db),
                     handlerStartup.Start(SubscriberGroup.Rep));
                    while (true)
                    {
                        // var actor = client.GetGrain<IAccount>(0);
                        // Console.WriteLine("Press Enter for times...");
                        Console.WriteLine("start");
                        Console.ReadLine();
                        var length = 10;// int.Parse(Console.ReadLine());
                        var stopWatch = new Stopwatch();
                        stopWatch.Start();
                        await Task.WhenAll(Enumerable.Range(0, length).Select(x => client.GetGrain<IAccount>(1).AddAmount(1000)));
                        stopWatch.Stop();
                        Console.WriteLine($"{length }次操作完成，耗时:{stopWatch.ElapsedMilliseconds}ms");
                        await Task.Delay(200);
                        Console.WriteLine($"余额为{await client.GetGrain<IAccount>(1).GetBalance()}");
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return 1;
            }
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
                            servicecollection.AddMQHandler();//注册所有handler
                            servicecollection.AddRay();//注册Client获取方法
                            servicecollection.AddSingleton<ISerializer, ProtobufSerializer>();//注册序列化组件
                            servicecollection.AddRabbitMQ();//注册RabbitMq为默认消息队列
                            servicecollection.AddLogging(logging => logging.AddConsole());
                            servicecollection.PostConfigure<RabbitConfig>(c =>
                            {
                                c.UserName = "admin";
                                c.Password = "admin";
                                c.Hosts = new[] { "192.168.125.230:5672" };
                                c.MaxPoolSize = 100;
                                c.VirtualHost = "ray";
                            });
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
