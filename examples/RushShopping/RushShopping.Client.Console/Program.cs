using System;
using Microsoft.Extensions.DependencyInjection;
using RushShopping.Repository;
using Microsoft.Extensions.Configuration;
using System.IO;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Orleans;
using Orleans.Runtime;
using RushShopping.Application;
using RushShopping.IGrains;
using RushShopping.Share.Dto;

namespace RushShopping.Client.Console
{
    class Program
    {
        public static IConfigurationRoot Configuration;

        static void Main()
        {
            RunMainAsync().Wait();
        }

        private static async Task RunMainAsync()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");
            Configuration = builder.Build();
            var client = StartClientWithRetries().Result;
            var customerGrain = client.GetGrain<ICustomerGrain<CustomerDto>>(Guid.NewGuid());
            await customerGrain.Create(new CustomerDto()
            {
                Name = "user1",
                Balance = 10
            });
            var customerDto = await customerGrain.Get();
            System.Console.WriteLine(customerDto.Name);
            System.Console.ReadKey();
        }

        public static IServiceProvider GetServiceProvider()
        {
            var services = new ServiceCollection();
            services.AddDbContext<RushShoppingDbContext>(
                    option => { option.UseNpgsql(Configuration.GetConnectionString("RushShoppingConnection")); },
                    ServiceLifetime.Transient)
                .AddEntityFrameworkNpgsql();
            services.AddTransient<IRushShoppingService, RushShoppingService>();
            return services.BuildServiceProvider();
        }

        public static async Task<IClusterClient> StartClientWithRetries(int initializeAttemptsBeforeFailing = 5)
        {
            int attempt = 0;
            IClusterClient client;
            while (true)
            {
                try
                {
                    var builder = new ClientBuilder()
                        .UseLocalhostClustering(30005)
                        .ConfigureApplicationParts(parts =>
                            parts.AddApplicationPart(typeof(ICustomerGrain<>).Assembly).WithReferences());
                    client = builder.Build();
                    await client.Connect();
                    System.Console.WriteLine("Client successfully connect to silo host");
                    break;
                }
                catch (SiloUnavailableException)
                {
                    attempt++;
                    System.Console.WriteLine(
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