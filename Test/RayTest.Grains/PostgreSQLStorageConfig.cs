using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Storage;
using Ray.Storage.PostgreSQL;

namespace RayTest.Grains
{
    public static class PostgreSQLStorageConfig
    {
        public static IServiceCollection PSQLConfigure(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IConfigureBuilder<long, Account>>(new PSQLConfigureBuilder<long, Account>((provider, id, parameter) =>
            new LongStorageOptions(provider, "core_event", "account")));

            return serviceCollection;
        }
    }
}
