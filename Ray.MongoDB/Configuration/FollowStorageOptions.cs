using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Storage;

namespace Ray.Storage.Mongo.Configuration
{
    public class FollowStorageOptions : IFollowStorageOptions
    {
        StorageOptions _baseConfig;
        public IStorageOptions Config
        {
            get => _baseConfig;
            set
            {
                _baseConfig = value as StorageOptions;
            }
        }
        public string FollowName { get; set; }
        public string FollowSnapshotTable => $"{_baseConfig.SnapshotCollection}_{FollowName}";

        public ValueTask Build()
        {
            return new ValueTask(CreateFollowSnapshotIndex());
        }
        private async Task CreateFollowSnapshotIndex()
        {
            var stateCollection = _baseConfig.Client.GetCollection<BsonDocument>(_baseConfig.DataBase, FollowSnapshotTable);
            var stateIndex = await stateCollection.Indexes.ListAsync();
            var stateIndexList = await stateIndex.ToListAsync();
            if (!stateIndexList.Exists(p => p["name"] == "State"))
            {
                await stateCollection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'StateId':1}", new CreateIndexOptions { Name = "State", Unique = false }));
            }
        }
    }
}
