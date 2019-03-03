using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Storage;
using Ray.Storage.PostgreSQL;

namespace RayTest.Grains
{
    public static class PostgreSQLStorageConfig
    {
        public static IServiceCollection PSQLConfigure(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IConfigureBuilder<long, Account>>(new SQLConfigureBuilder<long, Account>((provider, id, parameter) =>
            new StorageConfig(provider, "core_event", "account_event", "account_state")));

            return serviceCollection;
        }
    }
}
