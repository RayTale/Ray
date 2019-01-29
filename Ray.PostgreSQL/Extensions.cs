using Microsoft.Extensions.DependencyInjection;
using Ray.Core;
using Ray.Core.Storage;

namespace Ray.Storage.PostgreSQL
{
    public static class Extensions
    {
        public static void AddPostgreSQLStorage<PostgreSQLStorageConfig>(this IServiceCollection serviceCollection)
            where PostgreSQLStorageConfig : class, IStorageConfiguration<StorageConfig, ConfigParameter>
        {
            serviceCollection.AddSingleton<IBaseStorageFactory<StorageConfig>, StorageFactory>();
            serviceCollection.AddSingleton<IStorageConfiguration<StorageConfig, ConfigParameter>, PostgreSQLStorageConfig>();
            Startup.Register(serviceProvider =>
            {
                return serviceProvider.GetService<IStorageConfiguration<StorageConfig, ConfigParameter>>().
                Configure(serviceProvider.GetService<IConfigureBuilderContainer>());
            });
        }
    }
}
