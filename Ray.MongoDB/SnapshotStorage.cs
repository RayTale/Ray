using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;
using Ray.Core.Storage;

namespace Ray.Storage.MongoDB
{
    public class SnapshotStorage<PrimaryKey, Snapshot> : ISnapshotStorage<PrimaryKey, Snapshot>
        where Snapshot : class, new()
    {
        readonly StorageConfig grainConfig;
        readonly ISerializer serializer;
        public SnapshotStorage(ISerializer serializer, StorageConfig grainConfig)
        {
            this.serializer = serializer;
            this.grainConfig = grainConfig;
        }
        public Task Delete(PrimaryKey id)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", id);
            return grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).DeleteManyAsync(filter);
        }
        public async Task<Snapshot<PrimaryKey, Snapshot>> Get(PrimaryKey id)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", id);
            var cursor = await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).FindAsync<BsonDocument>(filter);
            var document = await cursor.FirstOrDefaultAsync();
            if (document != null)
            {
                return new Snapshot<PrimaryKey, Snapshot>()
                {
                    Base = new SnapshotBase<PrimaryKey>
                    {
                        StateId = id,
                        Version = document["Version"].AsInt64,
                        DoingVersion = document["Version"].AsInt64,
                        IsLatest = document["IsLatest"].AsBoolean,
                        IsOver = document["IsOver"].AsBoolean,
                        StartTimestamp = document["StartTimestamp"].AsInt64,
                        LatestMinEventTimestamp = document["LatestMinEventTimestamp"].AsInt64
                    },
                    State = serializer.Deserialize<Snapshot>(document["Data"].AsString)
                };
            }
            return default;
        }

        public async Task Insert(Snapshot<PrimaryKey, Snapshot> snapshot)
        {
            var doc = new BsonDocument
            {
                { "StateId", BsonValue.Create(snapshot.Base.StateId) },
                { "Version", snapshot.Base.Version },
                { "Data",  serializer.SerializeToString(snapshot.State) },
                { "StartTimestamp", snapshot.Base.StartTimestamp },
                { "LatestMinEventTimestamp", snapshot.Base.LatestMinEventTimestamp },
                { "IsLatest", snapshot.Base.IsLatest },
                { "IsOver", snapshot.Base.IsOver }
            };
            await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).InsertOneAsync(doc, null, new CancellationTokenSource(3000).Token);
        }

        public Task Over(PrimaryKey id, bool isOver)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", id);
            var update = Builders<BsonDocument>.Update.Set("IsOver", isOver);
            return grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).UpdateOneAsync(filter, update);
        }

        public async Task Update(Snapshot<PrimaryKey, Snapshot> snapshot)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", snapshot.Base.StateId);
            var json = serializer.SerializeToString(snapshot.State);
            if (!string.IsNullOrEmpty(json))
            {
                var update = Builders<BsonDocument>.Update.Set("Data", json).Set("Version", snapshot.Base.Version);
                await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).UpdateOneAsync(filter, update, null, new CancellationTokenSource(3000).Token);
            }
        }

        public Task UpdateIsLatest(PrimaryKey id, bool isLatest)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", id);
            var update = Builders<BsonDocument>.Update.Set("IsLatest", isLatest);
            return grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).UpdateOneAsync(filter, update);
        }

        public Task UpdateLatestMinEventTimestamp(PrimaryKey id, long timestamp)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", id);
            var update = Builders<BsonDocument>.Update.Set("LatestMinEventTimestamp", timestamp);
            return grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).UpdateOneAsync(filter, update);
        }

        public Task UpdateStartTimestamp(PrimaryKey id, long timestamp)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", id);
            var update = Builders<BsonDocument>.Update.Set("StartTimestamp", timestamp);
            return grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).UpdateOneAsync(filter, update);
        }
    }
}
