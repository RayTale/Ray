using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Ray.Core.Storage;
using Ray.Storage.PostgreSQL;

namespace Ray.Grain
{
    public class PostgreSQLStorageConfig : IStorageConfiguration<StorageConfig, ConfigParameter>
    {
        readonly IOptions<SqlConfig> options;
        public PostgreSQLStorageConfig(IOptions<SqlConfig> options) => this.options = options;
        public Task Configure(IConfigureBuilderContainer container)
        {
            new SQLConfigureBuilder<long>((grain, id, parameter) => new StorageConfig(options.Value.ConnectionDict["core_event"], "account_event", "account_state", parameter.IsFollow, parameter.FollowName)).
                Bind<Account>().Follow<AccountRep>().Follow<AccountDb>("db").Follow<AccountFlow>("flow").Complete(container);
            return Task.CompletedTask;
        }
    }
}
