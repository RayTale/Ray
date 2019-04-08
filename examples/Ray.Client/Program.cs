using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Ray.IGrains.Actors;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

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

            using (var client = await StartClientWithRetries())
            {
                while (true)
                {
                    try
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
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                }
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
                    var builder = new ClientBuilder()
                   .UseLocalhostClustering(30005)
                   .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(IAccount).Assembly).WithReferences())
                   .ConfigureLogging(logging => logging.AddConsole());
                    client = builder.Build();
                    await client.Connect();
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
