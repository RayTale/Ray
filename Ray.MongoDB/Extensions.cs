using System;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Storage.MongoDB
{
    public static class Extensions
    {
        public static void AddMongoDBStorage(this IServiceCollection serviceCollection, Action<MongoConnections> configAction)
        {
            serviceCollection.Configure<MongoConnections>(config => configAction(config));
            serviceCollection.AddSingleton<ICustomClient, CustomClient>();
            serviceCollection.AddSingleton<StorageFactory>();
        }
    }
}
