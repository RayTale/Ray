using System;
using Microsoft.Extensions.DependencyInjection;
using Ray.DistributedTx;

namespace Ray.Storage.MySQL
{
    public static class Extensions
    {
        public static void AddMySQLStorage(this IServiceCollection serviceCollection, Action<MySQLConnections> configAction)
        {
            serviceCollection.Configure<MySQLConnections>(config => configAction(config));
            serviceCollection.AddSingleton<StorageFactory>();
        }

        public static void AddMySQLTxStorage(this IServiceCollection serviceCollection, Action<TransactionOptions> configAction)
        {
            serviceCollection.Configure<TransactionOptions>(config => configAction(config));
            serviceCollection.AddSingleton<IDistributedTxStorage, DistributedTxStorage>();
        }
    }
}
