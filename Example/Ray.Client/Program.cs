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
using Ray.Core.Serialization;
using Ray.IGrains;
using Ray.IGrains.Actors;

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
                    while (true)
                    {
                        Console.WriteLine("start");
                        var times = int.Parse(Console.ReadLine());
                        var stopWatch = new Stopwatch();
                        stopWatch.Start();
                        await Task.WhenAll(Enumerable.Range(0, times).Select(x => client.GetGrain<IAccount>(1).AddAmount(1000)));
                        stopWatch.Stop();
                        Console.WriteLine($"{times }次操作完成，耗时:{stopWatch.ElapsedMilliseconds}ms");
                        await Task.Delay(200);
                        Console.WriteLine($"余额为{await client.GetGrain<IAccountRep>(1).GetBalance()}");
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
                    client = await ClusterClientFactory.Build(() =>
                    {
                        var builder = new ClientBuilder()
                        .UseLocalhostClustering()
                        .AddRay()
                        .ConfigureServices((context, servicecollection) =>
                        {
                            servicecollection.AddSingleton<ISerializer, ProtobufSerializer>();//注册序列化组件
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
