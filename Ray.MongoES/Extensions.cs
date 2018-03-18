using Microsoft.Extensions.DependencyInjection;
using Ray.Core.EventSourcing;

namespace Ray.MongoDb
{
    public static class Extensions
    {
        public static void AddMongoDb(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IMongoStorage, MongoStorage>();
            serviceCollection.AddSingleton<IStorageContainer, MongoStorageContainer>();
        }
    }
}
