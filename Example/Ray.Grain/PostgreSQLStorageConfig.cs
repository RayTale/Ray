using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ray.Core.Storage;
using Ray.Storage.PostgreSQL;

namespace Ray.Grain
{
    public static class PostgreSQLStorageConfig
    {
        public static IServiceCollection PSQLConfigure(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IConfigureBuilder<long, Account>>(new SQLConfigureBuilder<long, Account>((provider, id, parameter) =>
            new StorageConfig(provider.GetService<IOptions<SqlConfig>>().Value.ConnectionDict["core_event"], "account_event", "account_state", parameter.IsFollow, parameter.FollowName)).Follow<AccountRep>().Follow<AccountDb>("db").Follow<AccountFlow>("flow"));

            return serviceCollection;
        }
    }
}
