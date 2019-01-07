using Microsoft.Extensions.DependencyInjection;
using Ray.Core;
using Ray.Core.Storage;

namespace Ray.Storage.PostgreSQL
{
    public static class Extensions
    {
        public static void AddPostgreSQLStorage(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IStorageContainer, StorageContainer>();
            serviceCollection.AddSingleton(serviceProvider => serviceProvider.GetService<IStorageContainer>() as IConfigContainer);
            Startup.Register(serviceProvider =>
            {
                return serviceProvider.GetService<IStorageConfig>().Configure(serviceProvider.GetService<IConfigContainer>());
            });
        }
    }
}
