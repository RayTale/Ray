using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Transfer.IGrains;

namespace Transfer.Client
{
    class Program
    {
        static int Main(string[] args)
        {
            return RunMainAsync().Result;
        }
        private static async Task<int> RunMainAsync()
        {

            using var client = await StartClientWithRetries();
            Console.WriteLine($"账户1的初始余额为{await client.GetGrain<IAccount>(1).GetBalance()}");
            Console.WriteLine($"账户2的初始余额为{await client.GetGrain<IAccount>(2).GetBalance()}");
            while (true)
            {
                try
                {
                    Console.WriteLine("Please enter the number of executions");
                    var times = int.Parse(Console.ReadLine());
                    var topupWatch = new Stopwatch();
                    topupWatch.Start();
                    await Task.WhenAll(Enumerable.Range(0, times).Select(x => client.GetGrain<IAccount>(1).TopUp(100)));
                    topupWatch.Stop();
                    Console.WriteLine($"{times }次充值完成，耗时:{topupWatch.ElapsedMilliseconds}ms");
                    Console.WriteLine($"账户1的余额为{await client.GetGrain<IAccount>(1).GetBalance()}");
                    var transferWatch = new Stopwatch();
                    transferWatch.Start();
                    await Task.WhenAll(Enumerable.Range(0, times).Select(x => client.GetGrain<IAccount>(1).Transfer(2, 50)));
                    transferWatch.Stop();
                    Console.WriteLine($"{times }次转账完成，耗时:{transferWatch.ElapsedMilliseconds}ms");
                    Console.WriteLine($"账户1的余额为{await client.GetGrain<IAccount>(1).GetBalance()}");
                    await Task.Delay(1000);
                    Console.WriteLine($"账户2的余额为{await client.GetGrain<IAccount>(2).GetBalance()}");
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
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
                   .UseLocalhostClustering()
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
