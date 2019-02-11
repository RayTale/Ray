﻿using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Event;
using Ray.Core.Storage;
using Ray.EventBus.RabbitMQ;
using Ray.Storage.PostgreSQL;
using RayTest.Grains.EventHandles;
using RayTest.IGrains.States;

namespace RayTest.Grains
{
    public static class Extensions
    {
        public static void AddPSqlSiloGrain(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddMQService();
            serviceCollection.AddPostgreSQLStorage<PostgreSQLStorageConfig>();
            serviceCollection.AddSingleton<IStorageConfiguration<StorageConfig, ConfigParameter>, PostgreSQLStorageConfig>();
        }

        private static void AddMQService(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddRabbitMQ(container =>
            {
                return container.CreateEventBus<long>("Account", "account", 5).BindProducer<Account>().Enable();
            });
        }
    }
}
