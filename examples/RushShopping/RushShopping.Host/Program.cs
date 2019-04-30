using System;
using Microsoft.Extensions.Configuration;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.Extensions.DependencyInjection;
using RushShopping.Repository;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Ray.Core;
using Ray.Core.Event;
using Ray.Core.Storage;
using Ray.EventBus.RabbitMQ;
using Ray.Storage.PostgreSQL;
using Ray.Storage.SQLCore.Configuration;
using RushShopping.Grains;
using RushShopping.Grains.ProductGrains;

namespace RushShopping.Host
{
    class Program
    {
        private static ISiloHost _silo;
        private static readonly ManualResetEvent SiloStopped = new ManualResetEvent(false);

        static bool _siloStopping;
        static readonly object SyncLock = new object();
        public static IConfigurationRoot Configuration;
        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");
            Configuration = builder.Build();
            SetupApplicationShutdown();
            _silo = CreateSilo();
            StartSilo().Wait();
            SiloStopped.WaitOne();
        }

        private static ISiloHost CreateSilo()
        {
            var builder = new SiloHostBuilder()
                .Configure<ClusterOptions>(Configuration.GetSection("ClusterOptions"))
                .UseLocalhostClustering(11115, 30005)
                .AddRay<Configuration>()
                .ConfigureApplicationParts(
                    parts => parts.AddApplicationPart(typeof(CustomerGrain).Assembly).WithReferences())
                .ConfigureServices((context, serviceCollection) =>
                {
                    //注册postgresql为事件存储库
                    serviceCollection.AddPostgreSQLStorage(config =>
                    {
                        config.ConnectionDict.Add("core_event",
                            Configuration.GetConnectionString("EventConnection"));
                    });
                    serviceCollection.AddPostgreSQLTxStorage(options =>
                    {
                        options.ConnectionKey = "core_event";
                        options.TableName = "Transaction_TemporaryRecord";
                    });
                    serviceCollection.AddTransient(typeof(ICrudHandle<>), typeof(CrudHandle<,>));
                    serviceCollection.AddSingleton(typeof(IEventHandler<,>), typeof(CrudHandle<,>));
                    serviceCollection.AddAutoMapper(RushShoppingMapper.CreateMapping);
                    serviceCollection.AddSingleton<IConfigureBuilder<Guid, CustomerGrain>>(new PSQLConfigureBuilder<Guid, CustomerGrain>((provider, id, parameter) =>
                        new GuidKeyOptions(provider, "core_event", "customer")).AutoRegistrationObserver());
                    serviceCollection.AddSingleton<IConfigureBuilder<Guid, ProductGrain>>(new PSQLConfigureBuilder<Guid, ProductGrain>((provider, id, parameter) =>
                        new GuidKeyOptions(provider, "core_event", "product")).AutoRegistrationObserver());
                    serviceCollection.AddTransient(typeof(IGrainRepository<,>),typeof(GrainEfCoreRepositoryBase<,>));
                    serviceCollection.AddEntityFrameworkNpgsql().AddDbContext<RushShoppingDbContext>(
                    options =>
                    {
                        options.UseNpgsql(Configuration.GetConnectionString("RushShoppingConnection"));
                    }, ServiceLifetime.Transient);
                    serviceCollection.Configure<RabbitOptions>(Configuration.GetSection("RabbitConfig"));
                    serviceCollection.AddRabbitMQ(_ => { });
                })
                .Configure<GrainCollectionOptions>(options => { options.CollectionAge = TimeSpan.FromHours(2); })
                .ConfigureLogging(logging =>
                {
                    logging.SetMinimumLevel(LogLevel.Information);
                    logging.AddConsole(options => options.IncludeScopes = true);
                });

            var host = builder.Build();
            return host;
        }

        static void SetupApplicationShutdown()
        {
            Console.CancelKeyPress += (s, a) => {
                a.Cancel = true;
                lock (SyncLock)
                {
                    if (!_siloStopping)
                    {
                        _siloStopping = true;
                        Task.Run(StopSilo).Ignore();
                    }
                }
            };
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
