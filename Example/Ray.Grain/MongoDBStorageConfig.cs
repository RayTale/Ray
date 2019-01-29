using System.Threading.Tasks;
using Ray.Core.Storage;
using Ray.Storage.MongoDB;

namespace Ray.Grain
{
    public class MongoDBStorageConfig : IStorageConfiguration<StorageConfig, ConfigParameter>
    {
        readonly IMongoStorage mongoStorage;
        public MongoDBStorageConfig(IMongoStorage mongoStorage) => this.mongoStorage = mongoStorage;
        public Task Configure(IConfigureBuilderContainer container)
        {
            new MongoConfigureBuilder<long>((grain, id, parameter) => new StorageConfig(mongoStorage, "Ray", "account_event", parameter != default && !string.IsNullOrEmpty(parameter.SnapshotCollection) ? parameter.SnapshotCollection : "account_state")).
                AllotTo<Account>().AllotTo<AccountRep>().AllotTo<AccountDb>("account_db_state").AllotTo<AccountFlow>("account_flow_state").Complete(container);

            return Task.CompletedTask;
        }
    }
}
