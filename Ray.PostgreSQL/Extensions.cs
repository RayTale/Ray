using System;
using Microsoft.Extensions.DependencyInjection;
using Ray.DistributedTransaction;
using Ray.Storage.PostgreSQL.Services;
using Ray.Storage.PostgreSQL.Services.Abstractions;

namespace Ray.Storage.PostgreSQL
{
    public static class Extensions
    {
        public static void AddPostgreSQLStorage(this IServiceCollection serviceCollection, Action<SqlConfig> configAction)
        {
            serviceCollection.Configure<SqlConfig>(config => configAction(config));
            serviceCollection.AddSingleton<ITableRepository, TableRepository>();
            serviceCollection.AddSingleton<StorageFactory>();
        }
        public static void AddPostgreSQLTransactionStorage(this IServiceCollection serviceCollection, string connectionKey)
        {
            serviceCollection.Configure<TransactionStorageConfig>(config => config.ConnectionKey = connectionKey);
            serviceCollection.AddSingleton<ITransactionStorage, TransactionStorage>();
        }
    }
}
