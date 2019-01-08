using Microsoft.Extensions.DependencyInjection;
using Ray.Core;
using Ray.Core.Storage;

namespace Ray.Storage.MongoDB
{
    public static class Extensions
    {
        public static void AddMongoDBStorage(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IStorageContainer, StorageContainer>();
            Startup.Register(serviceProvider =>
            {
                return serviceProvider.GetService<IStorageConfiguration<StorageConfig, ConfigParameter>>().
                   Configure(serviceProvider.GetService<IConfigureContainer<StorageConfig, ConfigParameter>>());
            });
        }
    }
}
