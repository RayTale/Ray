using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Storage;

namespace Ray.Storage.Mongo.Configuration
{
    public class ObserverStorageOptions : IObserverStorageOptions
    {
        private StorageOptions baseConfig;

        public IStorageOptions Config
        {
            get => this.baseConfig;
            set => this.baseConfig = value as StorageOptions;
        }

        public string ObserverName { get; set; }

        public string ObserverSnapshotTable => $"{this.baseConfig.SnapshotCollection}_{this.ObserverName}";

        public ValueTask Build()
        {
            return new ValueTask(this.CreateObserverSnapshotIndex());
        }

        private async Task CreateObserverSnapshotIndex()
        {
            var stateCollection = this.baseConfig.Client.GetCollection<BsonDocument>(this.baseConfig.DataBase, this.ObserverSnapshotTable);
            var stateIndex = await stateCollection.Indexes.ListAsync();
            var stateIndexList = await stateIndex.ToListAsync();
            if (!stateIndexList.Exists(p => p["name"] == "State"))
            {
                await stateCollection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'StateId':1}", new CreateIndexOptions { Name = "State", Unique = false }));
            }
        }
    }
}
