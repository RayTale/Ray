using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Storage;

namespace Ray.Storage.Mongo.Configuration
{
    public class ObserverStorageOptions : IObserverStorageOptions
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
        public string ObserverName { get; set; }
        public string ObserverSnapshotTable => $"{_baseConfig.SnapshotCollection}_{ObserverName}";

        public ValueTask Build()
        {
            return new ValueTask(CreateObserverSnapshotIndex());
        }
        private async Task CreateObserverSnapshotIndex()
        {
            var stateCollection = _baseConfig.Client.GetCollection<BsonDocument>(_baseConfig.DataBase, ObserverSnapshotTable);
            var stateIndex = await stateCollection.Indexes.ListAsync();
            var stateIndexList = await stateIndex.ToListAsync();
            if (!stateIndexList.Exists(p => p["name"] == "State"))
            {
                await stateCollection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'StateId':1}", new CreateIndexOptions { Name = "State", Unique = false }));
            }
        }
    }
}
