using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core;
using Ray.Core.Abstractions;
using Ray.Core.Event;
using Ray.Core.Serialization;
using Ray.Core.Storage;
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
            serviceCollection.Configure();
            serviceCollection.AddMQService();
            serviceCollection.AddPostgreSQLStorage<PostgreSQLStorageConfig>();
            FollowUnitRegister();
        }
        public static void AddMongoDbSiloGrain(this IServiceCollection serviceCollection)
        {
            serviceCollection.Configure();
            serviceCollection.AddMQService();
            serviceCollection.AddMongoDBStorage<MongoDBStorageConfig>();
            FollowUnitRegister();
        }
        public static void FollowUnitRegister()
        {
            Startup.Register(serviceProvider =>
            {
                Configuration.ConfigureFollowUnit(serviceProvider, serviceProvider.GetService<IFollowUnitContainer>());
                return Task.CompletedTask;
            }, -1);
        }
        private static void AddMQService(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddRabbitMQ(async container =>
            {
                await container.CreateEventBus<Account>("Account", "account", 5).DefaultConsumer<long>();
            });
        }
    }
}
