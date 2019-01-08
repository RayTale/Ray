using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Ray.Core.Storage;
using Ray.Storage.PostgreSQL;

namespace RayTest.Grains
{
    public class PostgreSQLStorageConfig :   IStorageConfiguration<StorageConfig, ConfigParameter>
    {
        readonly IOptions<SqlConfig> options;
        public PostgreSQLStorageConfig(IOptions<SqlConfig> options)
        {
            this.options = options;
        }
        public Task Configure(IConfigureContainer<StorageConfig, ConfigParameter> container)
        {
            new SQLConfigureBuilder<long>((grain, id, parameter) => new StorageConfig(options.Value.ConnectionDict["core_event"], "account_event", parameter != default && !string.IsNullOrEmpty(parameter.SnapshotTable) ? parameter.SnapshotTable : "account_state")).
                BindTo<Account>();

            return Task.CompletedTask;
        }
    }
}
