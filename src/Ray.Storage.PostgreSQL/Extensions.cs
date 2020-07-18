using System;
using Microsoft.Extensions.DependencyInjection;
using Ray.DistributedTx;

namespace Ray.Storage.PostgreSQL
{
    public static class Extensions
    {
        public static void AddPostgreSQLStorage(this IServiceCollection serviceCollection, Action<PSQLConnections> configAction)
        {
            serviceCollection.Configure<PSQLConnections>(config => configAction(config));
            serviceCollection.AddSingleton<StorageFactory>();
        }
        public static void AddPostgreSQLTxStorage(this IServiceCollection serviceCollection, Action<TransactionOptions> configAction)
        {
            serviceCollection.Configure<TransactionOptions>(config => configAction(config));
            serviceCollection.AddSingleton<IDistributedTxStorage, DistributedTxStorage>();
        }
    }
}
