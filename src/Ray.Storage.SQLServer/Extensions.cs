using System;
using Microsoft.Extensions.DependencyInjection;
using Ray.DistributedTransaction;

namespace Ray.Storage.SQLServer
{
    public static class Extensions
    {
        public static void AddSQLServerStorage(this IServiceCollection serviceCollection, Action<SQLServerConnections> configAction)
        {
            serviceCollection.Configure<SQLServerConnections>(config => configAction(config));
            serviceCollection.AddSingleton<StorageFactory>();
        }
        public static void AddSQLServerTxStorage(this IServiceCollection serviceCollection, Action<TransactionOptions> configAction)
        {
            serviceCollection.Configure<TransactionOptions>(config => configAction(config));
            serviceCollection.AddSingleton<IDistributedTxStorage, DistributedTxStorage>();
        }
    }
}
