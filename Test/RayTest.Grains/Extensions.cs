using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Abstractions;
using Ray.EventBus.RabbitMQ;
using RayTest.Grains.EventHandles;
using RayTest.IGrains;
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
            serviceCollection.AddSingleton<IEventBusStartup<MessageInfo>, EventBusStartup>();
            serviceCollection.AddRabbitMQ<MessageInfo>();
        }
        public static void AddGrainHandler(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IEventHandler<AccountState>, AccountEventHandle>();
        }
    }
}
