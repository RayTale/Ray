using System;
using Microsoft.Extensions.DependencyInjection;
using Ray.DistributedTransaction;

namespace Ray.Storage.PostgreSQL
{
    public static class Extensions
    {
        public static void AddPostgreSQLStorage(this IServiceCollection serviceCollection, Action<PSQLConnections> configAction)
        {
            serviceCollection.Configure<PSQLConnections>(config => configAction(config));
            serviceCollection.AddSingleton<StorageFactory>();
        }
        public static void AddPostgreSQLTransactionStorage(this IServiceCollection serviceCollection, string connectionKey)
        {
            serviceCollection.Configure<TransactionStorageConfig>(config => config.ConnectionKey = connectionKey);
            serviceCollection.AddSingleton<ITransactionStorage, TransactionStorage>();
        }
    }
}
