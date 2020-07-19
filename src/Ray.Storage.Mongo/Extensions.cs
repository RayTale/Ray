using System;
using Microsoft.Extensions.DependencyInjection;
using Ray.DistributedTx;
using Ray.Storage.Mongo.Core;
using Ray.Storage.Mongo.Storage;

namespace Ray.Storage.Mongo
{
    public static class Extensions
    {
        public static void AddMongoDBStorage(this IServiceCollection serviceCollection, Action<MongoConnections> configAction)
        {
            serviceCollection.Configure<MongoConnections>(config => configAction(config));
            serviceCollection.AddSingleton<ICustomClient, CustomClient>();
            serviceCollection.AddSingleton<IIndexBuildService, IndexBuildService>();
            serviceCollection.AddSingleton<StorageFactory>();
        }

        public static void AddMongoTransactionStorage(this IServiceCollection serviceCollection, Action<TransactionOptions> configAction)
        {
            serviceCollection.Configure<TransactionOptions>(config => configAction(config));
            serviceCollection.AddSingleton<IIndexBuildService, IndexBuildService>();
            serviceCollection.AddSingleton<IDistributedTxStorage, DistributedTxStorage>();
        }
    }
}
