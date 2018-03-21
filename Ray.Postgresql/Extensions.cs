using Microsoft.Extensions.DependencyInjection;
using Ray.Core.EventSourcing;

namespace Ray.PostgreSQL
{
    public static class Extensions
    {
        public static void AddPostgresql(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IStorageContainer, SqlStorageContainer>();
        }
    }
}
