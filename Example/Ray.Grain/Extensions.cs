using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Event;
using Ray.Core.Storage;
using Ray.EventBus.RabbitMQ;
using Ray.Grain.EventHandles;
using Ray.IGrains;
using Ray.IGrains.Actors;
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
            serviceCollection.AddSingleton<IStorageConfiguration<Storage.PostgreSQL.StorageConfig, Storage.PostgreSQL.ConfigParameter>, PostgreSQLStorageConfig>();
            serviceCollection.AddGrainHandler();
        }
        public static void AddMongoDbSiloGrain(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddMQService();
            serviceCollection.AddMongoDBStorage();
            serviceCollection.AddSingleton<IMongoStorage, MongoStorage>();
            serviceCollection.AddSingleton<IStorageConfiguration<Storage.MongoDB.StorageConfig, Storage.MongoDB.ConfigParameter>, MongoDBStorageConfig>();
            serviceCollection.AddGrainHandler();
        }
        public static void AddGrainHandler(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IEventHandler<AccountState>, AccountEventHandle>();
        }
        private static void AddMQService(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddRabbitMQ<MessageInfo>(async container =>
            {
                await container.CreateEventBus<long>("Account", "account", 5).BindProducer<Account>().
                     CreateConsumer<long>(DefaultPrefix.primary).BindConcurrentFollowWithLongId<IAccountFlow>().BindFollowWithLongId<IAccountRep>().Complete().
                     CreateConsumer<long>(DefaultPrefix.secondary).BindConcurrentFollowWithLongId<IAccountDb>().Complete()
                 .Enable();
            });
        }
    }
}
