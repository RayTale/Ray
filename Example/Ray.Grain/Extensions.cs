using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core;
using Ray.Core.Abstractions;
using Ray.EventBus.RabbitMQ;
using Ray.Storage.MongoDB;
using Ray.Storage.PostgreSQL;

namespace Ray.Grain
{
    public static class Extensions
    {
        public static void AddPSqlSiloGrain(this IServiceCollection serviceCollection, Action<SqlConfig> configAction)
        {
            serviceCollection.Configure<SqlConfig>(config => configAction(config));
            serviceCollection.Configure();
            serviceCollection.AddPostgreSQLStorage<PostgreSQLStorageConfig>();
            FollowUnitRegister();
        }
        public static void AddMongoDbSiloGrain(this IServiceCollection serviceCollection, Action<MongoConfig> configAction)
        {
            serviceCollection.Configure<MongoConfig>(config => configAction(config));
            serviceCollection.Configure();
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
        public static void AddRabbitMQService(this IServiceCollection serviceCollection, Action<RabbitConfig> configAction)
        {
            serviceCollection.Configure<RabbitConfig>(config => configAction(config));
            serviceCollection.AddRabbitMQ(async container =>
            {
                await container.CreateEventBus<Account>("Account", "account", 5).DefaultConsumer<long>();
            });
        }
    }
}
