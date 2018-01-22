using Microsoft.Extensions.DependencyInjection;
using Ray.Core.EventSourcing;

namespace Ray.PostgresqlES
{
    public static class Extensions
    {
        public static void AddSqlES(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IStorageContainer, StorageContainer>();
        }
    }
}
