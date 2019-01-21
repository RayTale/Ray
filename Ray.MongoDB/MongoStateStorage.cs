using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Serialization;
using Ray.Core.State;
using Ray.Core.Storage;
using Ray.Core.Utils;

namespace Ray.Storage.MongoDB
{
    public class MongoStateStorage<K, S, B> : IStateStorage<K, S, B>
        where S : IState<K, B>, new()
        where B : IStateBase<K>, new()
    {
        readonly StorageConfig grainConfig;
        readonly ISerializer serializer;
        public MongoStateStorage(ISerializer serializer, StorageConfig grainConfig)
        {
            this.serializer = serializer;
            this.grainConfig = grainConfig;
        }
        public async Task Delete(K id)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("StateId", id);
            await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).DeleteManyAsync(filter);
        }
        public async Task<S> Get(K id)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("StateId", id);
            var cursor = await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).FindAsync<BsonDocument>(filter);
            var document = await cursor.FirstOrDefaultAsync();
            S result = default;
            if (document != null)
            {
                var data = document["Data"]?.AsByteArray;
                if (data != null)
                {
                    using (var ms = new MemoryStream(data))
                    {
                        result = serializer.Deserialize<S>(ms); ;
                    }
                }
            }
            return result;
        }

        public async Task Insert(S data)
        {
            var mState = new MongoState<K>
            {
                StateId = data.Base.StateId,
                Id = ObjectId.GenerateNewId().ToString(),
                Version = data.Base.Version
            };
            using (var ms = new PooledMemoryStream())
            {
                serializer.Serialize<S>(ms, data);
                mState.Data = ms.ToArray();
            }
            if (mState.Data != null && mState.Data.Count() > 0)
                await grainConfig.Storage.GetCollection<MongoState<K>>(grainConfig.DataBase, grainConfig.SnapshotCollection).InsertOneAsync(mState, null, new CancellationTokenSource(3000).Token);
        }

        public Task Over(K id)
        {
            //TODO 实现Over
            throw new System.NotImplementedException();
        }

        public async Task Update(S data)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("StateId", data.Base.StateId);
            byte[] bytes;
            using (var ms = new PooledMemoryStream())
            {
                serializer.Serialize<S>(ms, data);
                bytes = ms.ToArray();
            }
            if (bytes != null && bytes.Count() > 0)
            {
                var update = Builders<BsonDocument>.Update.Set("Data", bytes).Set("Version", data.Base.Version);
                await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).UpdateOneAsync(filter, update, null, new CancellationTokenSource(3000).Token);
            }
        }
    }
}
