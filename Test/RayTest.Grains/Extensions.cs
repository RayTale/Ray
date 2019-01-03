using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions;
using Ray.Core.EventBus;
using Ray.RabbitMQ;
using RayTest.Grains.EventHandles;
using RayTest.IGrains.States;

namespace RayTest.Grains
{
    public static class Extensions
    {
        public static void AddPSqlSiloGrain(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddMQService();
            serviceCollection.AddSingleton<IStorageContainer, PSQLStorageContainer>();
        }

        private static void AddMQService(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddRabbitMQ();
            serviceCollection.AddSingleton<IProducerContainer, ProducerContainer>();
        }
        public static void AddGrainHandler(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IEventHandler<AccountState>, AccountEventHandle>();
        }
    }
}
