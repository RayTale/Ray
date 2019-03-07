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
        public static void AddPostgreSQLTransactionStorage(this IServiceCollection serviceCollection, Action<TransactionOptions> configAction)
        {
            serviceCollection.Configure<TransactionOptions>(config => configAction(config));
            serviceCollection.AddSingleton<ITransactionStorage, TransactionStorage>();
        }
    }
}
