﻿using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Ray.Core;
using Ray.EventBus.RabbitMQ;
using Ray.EventBus.Kafka;
using Ray.Grain;
using Ray.Grain.EventHandles;
using Ray.Storage.Mongo;
using Ray.Storage.MySQL;
using Ray.Storage.PostgreSQL;
using Ray.Storage.SQLServer;

namespace Ray.MongoHost
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
                using (var host = await StartSilo())
                {
                    while (true)
                    {
                        Console.WriteLine("Input any key to stop");
                        Console.ReadLine();
                        await host.StopAsync();
                        Console.WriteLine("Input any key to Start");
                        Console.ReadLine();
                        await host.StartAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return 1;
            }
        }
        private static async Task<ISiloHost> StartSilo()
        {
            var builder = new SiloHostBuilder()
                .UseLocalhostClustering()
                .UseDashboard()
                .AddRay<Configuration>()
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback)
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(Account).Assembly).WithReferences())
                .ConfigureServices((context, servicecollection) =>
                {
                    //注册postgresql为事件存储库
                    //servicecollection.AddPostgreSQLStorage(config =>
                    //{
                    //    config.ConnectionDict.Add("core_event", "Server=127.0.0.1;Port=5432;Database=Ray;User Id=postgres;Password=extop;Pooling=true;MaxPoolSize=20;");
                    //});
                    //servicecollection.AddPostgreSQLTxStorage(options =>
                    //{
                    //    options.ConnectionKey = "core_event";
                    //    options.TableName = "Transaction_TemporaryRecord";
                    //});
                    //servicecollection.PSQLConfigure();
                    servicecollection.AddSQLServerStorage(config =>
                    {
                        config.ConnectionDict.Add("core_event", "Server=127.0.0.1,1433;Database=Ray;User Id=sa;Password=luohuazhiyu;Pooling=true;max pool size=20;");
                    });
                    servicecollection.AddSQLServerTxStorage(options =>
                    {
                        options.ConnectionKey = "core_event";
                        options.TableName = "Transaction_TemporaryRecord";
                    });
                    servicecollection.SQLServerConfigure();
                    //注册mysql作为事件存储库
                    //servicecollection.AddMySQLStorage(config =>
                    //{
                    //    config.ConnectionDict.Add("core_event", "Server=127.0.0.1;Port=3306;Database=ray;User Id=root;Password=extop;Pooling=true;MaxPoolSize=20;");
                    //});
                    //servicecollection.AddMySQLTxStorage(options =>
                    //{
                    //    options.ConnectionKey = "core_event";
                    //    options.TableName = "Transaction_TemporaryRecord";
                    //});
                    //servicecollection.MySQLConfigure();
                    //注册mongodb为事件存储库
                    //servicecollection.AddMongoDBStorage(config =>
                    //{
                    //    config.ConnectionDict.Add("core", "mongodb://127.0.0.1:27017");
                    //});
                    //servicecollection.AddMongoTransactionStorage(options =>
                    //{
                    //    options.ConnectionKey = "core";
                    //    options.CollectionName = "Transaction_TemporaryRecord";
                    //});
                    //servicecollection.MongoConfigure();
                    servicecollection.AddRabbitMQ(config =>
                    {
                        config.UserName = "admin";
                        config.Password = "admin";
                        config.Hosts = new[] { "127.0.0.1:5672" };
                        config.MaxPoolSize = 100;
                        config.VirtualHost = "/";
                    });
                    //servicecollection.AddKafkaMQ(
                    //    config => { },
                    //    config =>
                    //    {
                    //        config.BootstrapServers = "192.168.1.2:9092";
                    //    }, config =>
                    //    {
                    //        config.BootstrapServers = "192.168.1.2:9092";
                    //    }, async container =>
                    //    {
                    //        await container.CreateEventBus<Account>(nameof(Account), 5).DefaultConsumer<long>();
                    //    });
                    //servicecollection.AddRabbitMQ(config =>
                    //{
                    //    config.UserName = "admin";
                    //    config.Password = "admin";
                    //    config.Hosts = new[] { "127.0.0.1:5672" };
                    //    config.MaxPoolSize = 100;
                    //    config.VirtualHost = "/";
                    //}, async container =>
                    //{
                    //    await container.CreateEventBus<Account>("Account", "account", 5).DefaultConsumer<long>();
                    //});
                })
                 .Configure<GrainCollectionOptions>(options =>
                 {
                     options.CollectionAge = TimeSpan.FromMinutes(5);
                 })
                .ConfigureLogging(logging =>
                {
                    logging.SetMinimumLevel(LogLevel.Information);
                    logging.AddConsole(options => options.IncludeScopes = true);
                });

            var host = builder.Build();
            await host.StartAsync();
            return host;
        }
    }
}
