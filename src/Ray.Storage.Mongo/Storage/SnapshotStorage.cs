using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;
using Ray.Core.Storage;

namespace Ray.Storage.Mongo
{
    public class SnapshotStorage<PrimaryKey, StateType> : ISnapshotStorage<PrimaryKey, StateType>
        where StateType : class, new()
    {
        readonly StorageOptions grainConfig;
        readonly ISerializer serializer;
        public SnapshotStorage(ISerializer serializer, StorageOptions grainConfig)
        {
            this.serializer = serializer;
            this.grainConfig = grainConfig;
        }
        public Task Delete(PrimaryKey id)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", id);
            return grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).DeleteManyAsync(filter);
        }
        public async Task<Snapshot<PrimaryKey, StateType>> Get(PrimaryKey id)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", id);
            var cursor = await grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).FindAsync<BsonDocument>(filter);
            var document = await cursor.FirstOrDefaultAsync();
            if (document != null)
            {
                return new Snapshot<PrimaryKey, StateType>()
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
                    State = serializer.Deserialize<StateType>(document["Data"].AsString)
                };
            }
            return default;
        }

        public async Task Insert(Snapshot<PrimaryKey, StateType> snapshot)
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
            using var tokenSource = new CancellationTokenSource(3000);
            await grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).InsertOneAsync(doc, null, tokenSource.Token);
        }

        public Task Over(PrimaryKey id, bool isOver)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", id);
            var update = Builders<BsonDocument>.Update.Set("IsOver", isOver);
            return grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).UpdateOneAsync(filter, update);
        }

        public async Task Update(Snapshot<PrimaryKey, StateType> snapshot)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", snapshot.Base.StateId);
            var json = serializer.SerializeToString(snapshot.State);
            if (!string.IsNullOrEmpty(json))
            {
                var update = 
                    Builders<BsonDocument>.Update.Set("Data", json).
                    Set("Version", snapshot.Base.Version).
                    Set("LatestMinEventTimestamp", snapshot.Base.LatestMinEventTimestamp).
                    Set("IsLatest", snapshot.Base.IsLatest).
                    Set("IsOver", snapshot.Base.IsOver);
                await grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).UpdateOneAsync(filter, update, null, new CancellationTokenSource(3000).Token);
            }
        }

        public Task UpdateIsLatest(PrimaryKey id, bool isLatest)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", id);
            var update = Builders<BsonDocument>.Update.Set("IsLatest", isLatest);
            return grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).UpdateOneAsync(filter, update);
        }

        public Task UpdateLatestMinEventTimestamp(PrimaryKey id, long timestamp)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", id);
            var update = Builders<BsonDocument>.Update.Set("LatestMinEventTimestamp", timestamp);
            return grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).UpdateOneAsync(filter, update);
        }

        public Task UpdateStartTimestamp(PrimaryKey id, long timestamp)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", id);
            var update = Builders<BsonDocument>.Update.Set("StartTimestamp", timestamp);
            return grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).UpdateOneAsync(filter, update);
        }
    }
}
