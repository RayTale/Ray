using System;
using Microsoft.Extensions.Configuration;
using System.IO;
using System.Net;
using AutoMapper;
using Microsoft.Extensions.DependencyInjection;
using RushShopping.Repository;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Ray.Core;
using Ray.EventBus.RabbitMQ;
using Ray.Storage.PostgreSQL;

namespace RushShopping.Host
{
    class Program
    {
        public static IConfigurationRoot Configuration;
        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");
            Configuration = builder.Build();
        }

        public static IServiceProvider GetServiceProvider()
        {
            var services = new ServiceCollection();
            services.AddDbContext<RushShoppingDbContext>(option =>
            {
                option.UseNpgsql(Configuration.GetConnectionString("RushShoppingConnection"));
            }, ServiceLifetime.Transient)
            .AddEntityFrameworkNpgsql();
            return services.BuildServiceProvider();
        }

        private static ISiloHost CreateSilo()
        {
            var builder = new SiloHostBuilder()
                .Configure<ClusterOptions>(Configuration.GetSection("ClusterOptions"))
                .UseLocalhostClustering()
                .UseDashboard()
                .AddRay<Grain.Configuration>()
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback)
                .ConfigureApplicationParts(
                    parts => parts.AddApplicationPart(typeof(WorkFlowGrain).Assembly).WithReferences())
                .ConfigureServices((context, serviceCollection) =>
                {
                    //注册postgresql为事件存储库
                    serviceCollection.AddPostgreSQLStorage(config =>
                    {
                        config.ConnectionDict.Add("core_event",
                            Configuration.GetConnectionString("CoreEvent"));
                    });
                    serviceCollection.AddPostgreSQLTxStorage(options =>
                    {
                        options.ConnectionKey = "core_event";
                        options.TableName = "Transaction_TemporaryRecord";
                    });
                    serviceCollection.AddAutoMapper(WorkFlowDtoMapper.CreateMapping);
                    serviceCollection.PSQLConfigure();
                    serviceCollection.AddSingleton<IWorkFlowCodeGenerator, WorkFlowCodeGenerator>();
                    serviceCollection.AddAbpRepository();
                    serviceCollection.AddEntityFrameworkNpgsql().AddDbContext<ApprovalDbContext>(
                        options =>
                        {
                            DbContextOptionsConfigurer.Configure(options,
                                _appConfiguration.GetConnectionString("Default"));
                        }, ServiceLifetime.Transient);
                    serviceCollection.Configure<RabbitOptions>(_appConfiguration.GetSection("RabbitConfig"));
                    serviceCollection.AddRabbitMQ(_ => { });
                })
                .AddIncomingGrainCallFilter<DbContextGrainCallFilter>()
                .Configure<GrainCollectionOptions>(options => { options.CollectionAge = TimeSpan.FromHours(2); })
                .ConfigureLogging(logging =>
                {
                    logging.SetMinimumLevel(LogLevel.Information);
                    logging.AddConsole(options => options.IncludeScopes = true);
                });

            var host = builder.Build();
            return host;
        }

        private static async Task StartSilo()
        {
            await _silo.StartAsync();
            Console.WriteLine("Silo started");
        }

        private static async Task StopSilo()
        {
            await _silo.StopAsync();
            Console.WriteLine("Silo stopped");
            SiloStopped.Set();
        }
    }
}
