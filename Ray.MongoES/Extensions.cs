using Microsoft.Extensions.DependencyInjection;
using Ray.Core.EventSourcing;

namespace Ray.MongoES
{
    public static class Extensions
    {
        public static void AddMongoES(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IStorageContainer, MongoStorageContainer>();
        }
    }
}
