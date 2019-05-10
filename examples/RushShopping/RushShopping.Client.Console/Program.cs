using System;
using Microsoft.Extensions.DependencyInjection;
using RushShopping.Repository;
using Microsoft.Extensions.Configuration;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Orleans;
using Orleans.Runtime;
using RushShopping.Application;
using RushShopping.IGrains;
using RushShopping.Repository.Entities;
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
            //对增删改查进行测试
            await CrudTests();
        }

        public static async Task CrudTests()
        {
            var serviceProvider = GetServiceProvider();
            var rushShoppingService = serviceProvider.GetService<IRushShoppingService>();
            var customerId = await rushShoppingService.CreateCustomer(new CustomerDto()
            {
                Name = "user1",
                Balance = 10
            });
            var grainCustomerDto = await rushShoppingService.GetCustomer(customerId);
            System.Console.WriteLine($"Grain customerDto.Name:{grainCustomerDto.Name}");
            await Task.Delay(100);
            Customer customer;
            using (var dbContext = serviceProvider.GetService<RushShoppingDbContext>())
            {
                 customer = await dbContext.Customers.Include(x => x.ProductOrders)
                    .FirstOrDefaultAsync(x => x.Id == customerId);
                System.Console.WriteLine($"DbContext customerDto.Name:{customer.Name}");
            }
          
            await rushShoppingService.UpdateCustomer(new CustomerDto()
            {
                Id = customer.Id,
                Balance = customer.Balance,
                Name = "user2",
                ProductOrders = customer.ProductOrders.Select(x => new ProductOrderDto()
                {
                    Id = x.Id,
                    BuyerId = x.BuyerId,
                    Price = x.Price,
                    ProductId = x.ProductId
                }).ToList()
            });
            grainCustomerDto = await rushShoppingService.GetCustomer(customerId);
            System.Console.WriteLine($"Grain customerDto.Name:{grainCustomerDto.Name}");
            await Task.Delay(100);
            using (var dbContext = serviceProvider.GetService<RushShoppingDbContext>())
            {
                customer = await dbContext.Customers.Include(x => x.ProductOrders)
                    .FirstOrDefaultAsync(x => x.Id == customerId);
                System.Console.WriteLine($"DbContext customerDto.Name:{customer.Name}");
            }
            await rushShoppingService.DeleteCustomer(customerId);
            System.Console.ReadKey();
        }

        public static IServiceProvider GetServiceProvider()
        {
            var services = new ServiceCollection();
            var clusterClient = StartClientWithRetries().Result;
            services.AddDbContext<RushShoppingDbContext>(
                    option => { option.UseNpgsql(Configuration.GetConnectionString("RushShoppingConnection")); },
                    ServiceLifetime.Transient)
                .AddEntityFrameworkNpgsql();
            services.AddTransient<IRushShoppingService, RushShoppingService>();
            services.AddSingleton(clusterClient);
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