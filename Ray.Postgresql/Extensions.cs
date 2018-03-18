using Microsoft.Extensions.DependencyInjection;
using Ray.Core.EventSourcing;

namespace Ray.Postgresql
{
    public static class Extensions
    {
        public static void AddPostgresql(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IStorageContainer, StorageContainer>();
        }
    }
}
