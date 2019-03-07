using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Storage;
using Ray.Storage.Mongo;

namespace Ray.Grain
{
    public static class MongoDBStorageConfig
    {
        public static IServiceCollection MongoConfigure(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IConfigureBuilder<long, Account>>(new MongoConfigureBuilder<long, Account>((provider, id, parameter) => new StorageOptions(provider, "core", "Ray", "account")).
                Follow<AccountRep>().Follow<AccountDb>("db").Follow<AccountFlow>("flow"));

            return serviceCollection;
        }
    }
}
