using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using TxTransfer.IGrains;
using TxTransfer.IGrains.TransactionUnits;

namespace TxTransfer.Client
{
    class Program
    {
        static async Task Main()
        {
            using var client = await StartClientWithRetries();
            Console.WriteLine($"The starting balance of account 1 is{await client.GetGrain<IAccount>(1).GetBalance()}");
            Console.WriteLine($"The starting balance of account 2 is{await client.GetGrain<IAccount>(2).GetBalance()}");
            while (true)
            {
                try
                {
                    Console.WriteLine("Please enter the number of executions");
                    var times = int.Parse(Console.ReadLine() ?? "100");
                    var topUpStopwatch = new Stopwatch();
                    topUpStopwatch.Start();
                    await Task.WhenAll(Enumerable.Range(0, times).Select(x => client.GetGrain<IAccount>(1).TopUp(100)));
                    topUpStopwatch.Stop();
                    Console.WriteLine(
                        $"{times}Recharge completed, time-consuming:{topUpStopwatch.ElapsedMilliseconds}ms");
                    Console.WriteLine($"The balance of account 1 is{await client.GetGrain<IAccount>(1).GetBalance()}");
                    var transferWatch = new Stopwatch();
                    transferWatch.Start();
                    await Task.WhenAll(Enumerable.Range(0, times).Select(x =>
                        client.GetGrain<ITransferUnit>(0).Ask(new TransferInput {FromId = 1, ToId = 2, Amount = 50})));
                    transferWatch.Stop();
                    Console.WriteLine(
                        $"{times}The transfer is completed, time-consuming:{transferWatch.ElapsedMilliseconds}ms");
                    Console.WriteLine($"The balance of account 1 is{await client.GetGrain<IAccount>(1).GetBalance()}");
                    await Task.Delay(1000);
                    Console.WriteLine($"The balance of account 2 is{await client.GetGrain<IAccount>(2).GetBalance()}");
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
                        .ConfigureApplicationParts(parts =>
                            parts.AddApplicationPart(typeof(IAccount).Assembly).WithReferences())
                        .ConfigureLogging(logging => logging.AddConsole());
                    client = builder.Build();
                    await client.Connect();
                    Console.WriteLine("Client successfully connect to silo host");
                    break;
                }
                catch (SiloUnavailableException)
                {
                    attempt++;
                    Console.WriteLine(
                        $"Attempt {attempt} of {initializeAttemptsBeforeFailing} failed to initialize the Orleans client.");
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