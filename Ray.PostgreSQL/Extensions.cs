using Microsoft.Extensions.DependencyInjection;
using Ray.Core;
using Ray.Core.Storage;

namespace Ray.Storage.PostgreSQL
{
    public static class Extensions
    {
        public static void AddPostgreSQLStorage(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IBaseStorageFactory<StorageConfig>, StorageFactory>();
            Startup.Register(serviceProvider =>
            {
                return serviceProvider.GetService<IStorageConfiguration<StorageConfig, ConfigParameter>>().
                Configure(serviceProvider.GetService<IConfigureBuilderContainer>());
            });
        }
    }
}
