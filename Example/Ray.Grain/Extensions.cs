using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions;
using Ray.EventBus.RabbitMQ;
using Ray.Grain.EventHandles;
using Ray.IGrains;
using Ray.IGrains.States;
using Ray.Storage.MongoDB;
using Ray.Storage.PostgreSQL;

namespace Ray.Grain
{
    public static class Extensions
    {
        public static void AddPSqlSiloGrain(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddMQService();
            serviceCollection.AddPostgreSQLStorage();
            serviceCollection.AddSingleton<Storage.PostgreSQL.IStorageConfig, PostgreSQLStorageConfig>();
            serviceCollection.AddGrainHandler();
        }
        public static void AddMongoDbSiloGrain(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddMQService();
            serviceCollection.AddMongoDBStorage();
            serviceCollection.AddSingleton<IMongoStorage, MongoStorage>();
            serviceCollection.AddSingleton<Storage.MongoDB.IStorageConfig, MongoDBStorageConfig>();
            serviceCollection.AddGrainHandler();
        }
        public static void AddGrainHandler(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IEventHandler<AccountState>, AccountEventHandle>();
        }
        private static void AddMQService(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IEventBusConfig<MessageInfo>, EventBusStartup>();
            serviceCollection.AddRabbitMQ<MessageInfo>();
        }
    }
}
