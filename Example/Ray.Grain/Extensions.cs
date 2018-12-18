using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Internal;
using Ray.Core.EventBus;
using Ray.MongoDB;
using Ray.RabbitMQ;
using Ray.IGrains.States;
using Ray.Grain.EventHandles;

namespace Ray.Grain
{
    public static class Extensions
    {
        public static void AddPSqlSiloGrain(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddMQService();
            serviceCollection.AddSingleton<IStorageContainer, PSQLStorageContainer>();
            serviceCollection.AddGrainHandler();
        }
        public static void AddMongoDbSiloGrain(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddMQService();
            serviceCollection.AddSingleton<IMongoStorage, MongoStorage>();
            serviceCollection.AddSingleton<IStorageContainer, MongoStorageContainer>();
            serviceCollection.AddGrainHandler();
        }
        public static void AddGrainHandler(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IEventHandler<AccountState>, AccountEventHandle>();
        }
        private static void AddMQService(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddRabbitMQ();
            serviceCollection.AddSingleton<IProducerContainer, ProducerContainer>();
        }
    }
}
