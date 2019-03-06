using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Storage;

namespace Ray.Storage.MongoDB.Configuration
{
    public class FollowStorageConfig : IFollowStorageConfig
    {
        StorageConfig _baseConfig;
        public IStorageConfig Config
        {
            get => _baseConfig;
            set
            {
                _baseConfig = value as StorageConfig;
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
            var stateCollection = _baseConfig.Storage.GetCollection<BsonDocument>(_baseConfig.DataBase, FollowSnapshotTable);
            var stateIndex = await stateCollection.Indexes.ListAsync();
            var stateIndexList = await stateIndex.ToListAsync();
            if (!stateIndexList.Exists(p => p["name"] == "State"))
            {
                await stateCollection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'StateId':1}", new CreateIndexOptions { Name = "State", Unique = false }));
            }
        }
    }
}
