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
            new MongoConfigureBuilder<long>((grain, id, parameter) => new StorageConfig(mongoStorage, "Ray", "account_event", "account_state",parameter.IsFollow,parameter.FollowName)).
                Bind<Account>().Follow<AccountRep>().Follow<AccountDb>("db").Follow<AccountFlow>("flow").Complete(container);

            return Task.CompletedTask;
        }
    }
}
