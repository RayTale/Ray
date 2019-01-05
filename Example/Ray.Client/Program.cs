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
using Ray.Core.Abstractions;
using Ray.Handler;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.EventBus.RabbitMQ;
using Ray.Core.Abstractions.Actors;
using System.Collections.Generic;

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
            var mqListenerNodes = new List<string> { "N1" };
            try
            {
                using (var client = await StartClientWithRetries())
                {
                    var handlerStartup = client.ServiceProvider.GetService<HandlerStartup>();
                    foreach (var node in mqListenerNodes)
                    {
                      var (isOk, lockId) = await  client.GetGrain<INoWaitLock>(node).Lock();
                    }
                    await Task.WhenAll(
                     handlerStartup.Start(SubscriberGroup.Core),
                     handlerStartup.Start(SubscriberGroup.Db),
                     handlerStartup.Start(SubscriberGroup.Rep));
                    while (true)
                    {
                        // var actor = client.GetGrain<IAccount>(0);
                        // Console.WriteLine("Press Enter for times...");
                        Console.WriteLine("start");
                        var times = int.Parse(Console.ReadLine());
                        var stopWatch = new Stopwatch();
                        stopWatch.Start();
                        var ids = await Task.WhenAll(Enumerable.Range(0, times).Select(x => client.GetGrain<IUID>("order").NewLocalID()));
                        stopWatch.Stop();
                        var distinctIds = ids.Distinct();
                        if (ids.Count() != distinctIds.Count())
                        {
                            Console.WriteLine("id出现重复");
                        }
                        Console.WriteLine($"{times }个id生成完成，耗时:{stopWatch.ElapsedMilliseconds}ms");
                        await Task.Delay(200);
                        //Console.WriteLine($"余额为{await client.GetGrain<IAccount>(1).GetBalance()}");
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
                                c.Hosts = new[] { "127.0.0.1:5672" };
                                c.MaxPoolSize = 100;
                                c.VirtualHost = "/";
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
