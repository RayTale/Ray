using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Serialization;
using Ray.Core.State;
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
                        StateId = serializer.Deserialize<PrimaryKey>(document["StateId"].AsString),
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

        public async Task Insert(Snapshot<PrimaryKey, Snapshot> data)
        {
            var doc = new BsonDocument
            {
                { "StateId", BsonValue.Create(data.Base.StateId) },
                { "Version", data.Base.Version },
                { "Data",  serializer.SerializeToString(data.State) },
                { "StartTimestamp", data.Base.StartTimestamp },
                { "LatestMinEventTimestamp", data.Base.LatestMinEventTimestamp },
                { "IsLatest", data.Base.IsLatest },
                { "IsOver", data.Base.IsOver }
            };
            await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).InsertOneAsync(doc, null, new CancellationTokenSource(3000).Token);
        }

        public Task Over(PrimaryKey id, bool isOver)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", id);
            var update = Builders<BsonDocument>.Update.Set("IsOver", isOver);
            return grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).UpdateOneAsync(filter, update);
        }

        public async Task Update(Snapshot<PrimaryKey, Snapshot> data)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", data.Base.StateId);
            var json = serializer.SerializeToString(data.State);
            if (!string.IsNullOrEmpty(json))
            {
                var update = Builders<BsonDocument>.Update.Set("Data", json).Set("Version", data.Base.Version);
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
