using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Storage;
using Ray.Storage.PostgreSQL;
using Ray.Storage.MySQL;
using Ray.Storage.SQLCore.Configuration;

namespace Ray.Grain
{
    public static class SQLStorageConfig
    {
        public static IServiceCollection PSQLConfigure(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IConfigureBuilder<long, Account>>(new PSQLConfigureBuilder<long, Account>((provider, id, parameter) =>
            new IntegerKeyOptions(provider, "core_event", "account")).Observe<AccountRep>().Observe<AccountDb>("db").Observe<AccountFlow>("observer"));

            return serviceCollection;
        }
        public static IServiceCollection MySQLConfigure(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IConfigureBuilder<long, Account>>(new MySQLConfigureBuilder<long, Account>((provider, id, parameter) =>
            new IntegerKeyOptions(provider, "core_event", "account")).Observe<AccountRep>().Observe<AccountDb>("db").Observe<AccountFlow>("observer"));

            return serviceCollection;
        }
    }
}
