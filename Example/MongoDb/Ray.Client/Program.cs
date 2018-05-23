using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Ray.Handler;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.RabbitMQ;
using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Message;
using System.Diagnostics;
using Ray.Core.MQ;
using Ray.Core;

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
                    var manager = client.ServiceProvider.GetService<ISubManager>();
                    await manager.Start(new[] { "Core", "Read" });
                    var aActor = client.GetGrain<IAccount>(1);
                    var bActor = client.GetGrain<IAccount>(2);
                    while (true)
                    {
                        Console.WriteLine("Press Enter to terminate...");
                        var length = int.Parse(Console.ReadLine());
                        var stopWatch = new Stopwatch();
                        stopWatch.Start();
                        var tasks = new Task[length * 2];
                        Parallel.For(0, length, i =>
                        {
                            tasks[i * 2] = aActor.AddAmount(1000);//1用户充值1000
                            tasks[i * 2 + 1] = aActor.Transfer(2, 500);//转给2用户500
                        });
                        await Task.WhenAll(tasks);
                        stopWatch.Stop();
                        Console.WriteLine($"{length * 2}次操作完成，耗时:{stopWatch.ElapsedMilliseconds}ms");
                        await Task.Delay(200);

                        Console.WriteLine($"End:1的余额为{await aActor.GetBalance()},2的余额为{await bActor.GetBalance()}");
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
                    client = new ClientBuilder()
                     .UseLocalhostClustering()
                    .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(IAccount).Assembly).WithReferences())
                    .ConfigureLogging(logging => logging.AddConsole())
                    .ConfigureServices((servicecollection) =>
                    {
                        SubManager.Parse(servicecollection, typeof(AccountCoreHandler).Assembly);//注册handle
                        servicecollection.AddSingleton<IOrleansClientFactory, OrleansClientFactory>();//注册Client获取方法
                        servicecollection.AddSingleton<ISerializer, ProtobufSerializer>();//注册序列化组件
                        servicecollection.AddRabbitMQ<MessageInfo>();//注册RabbitMq为默认消息队列
                        servicecollection.PostConfigure<RabbitConfig>(c =>
                    {
                        c.UserName = "admin";
                        c.Password = "luohuazhiyu";
                        c.Hosts = new[] { "127.0.0.1:5672" };
                        c.MaxPoolSize = 100;
                        c.VirtualHost = "/";
                    });
                    })
                    .Build();
                    await client.ConnectAndFill();
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
