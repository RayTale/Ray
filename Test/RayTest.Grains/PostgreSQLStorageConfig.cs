using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Storage;
using Ray.Storage.PostgreSQL;
using Ray.Storage.SQLCore.Configuration;

namespace RayTest.Grains
{
    public static class PostgreSQLStorageConfig
    {
        public static IServiceCollection PSQLConfigure(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IConfigureBuilder<long, Account>>(new PSQLConfigureBuilder<long, Account>((provider, id, parameter) =>
            new IntegerKeyOptions(provider, "core_event", "account")).AutoRegistrationObserver());

            return serviceCollection;
        }
    }
}
