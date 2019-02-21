using System;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Storage.MongoDB
{
    public static class Extensions
    {
        public static void AddMongoDBStorage(this IServiceCollection serviceCollection, Action<MongoConfig> configAction)
        {
            serviceCollection.Configure<MongoConfig>(config => configAction(config));
            serviceCollection.AddSingleton<IMongoStorage, MongoStorage>();
            serviceCollection.AddSingleton<StorageFactory>();
        }
    }
}
