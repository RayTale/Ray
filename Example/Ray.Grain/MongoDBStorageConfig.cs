using System.Threading.Tasks;
using Ray.Storage.MongoDB;

namespace Ray.Grain
{
    public class MongoDBStorageConfig : IStorageConfig
    {
        readonly IMongoStorage mongoStorage;
        public MongoDBStorageConfig(IMongoStorage mongoStorage) => this.mongoStorage = mongoStorage;
        public Task Configure(IConfigContainer container)
        {
            container.CreateBuilder<long>((grain, id) => new MongoGrainConfig(mongoStorage, "Ray", "account_event", "account_state")).
                BindToGrain<Account>().BindToGrain<AccountRep>().BindToGrain<AccountDb>("account_db_state").BindToGrain<AccountFlow>("account_flow_state").Enable();

            return Task.CompletedTask;
        }
    }
}
