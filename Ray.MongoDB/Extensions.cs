using Microsoft.Extensions.DependencyInjection;
using Ray.Core.EventSourcing;

namespace Ray.MongoDB
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
