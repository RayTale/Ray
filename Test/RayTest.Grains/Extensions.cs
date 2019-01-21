using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Event;
using Ray.Core.Storage;
using Ray.EventBus.RabbitMQ;
using Ray.Storage.PostgreSQL;
using RayTest.Grains.EventHandles;
using RayTest.IGrains;
using RayTest.IGrains.Events;
using RayTest.IGrains.States;

namespace RayTest.Grains
{
    public static class Extensions
    {
        public static void AddPSqlSiloGrain(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddMQService();
            serviceCollection.AddPostgreSQLStorage();
            serviceCollection.AddSingleton<IStorageConfiguration<StorageConfig, ConfigParameter>, PostgreSQLStorageConfig>();
        }

        private static void AddMQService(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddRabbitMQ<MessageInfo>(container =>
            {
                return container.CreateEventBus<long>("Account", "account", 5).BindProducer<Account>().Enable();
            });
        }
        public static void AddGrainHandler(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IEventHandler<long, EventBase<long>, AccountState, StateBase<long>>, AccountEventHandle>();
        }
    }
}
