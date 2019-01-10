using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using ProtoBuf;
using Ray.Core.State;
using Ray.Core.Storage;
using Ray.Core.Utils;

namespace Ray.Storage.MongoDB
{
    public class MongoStateStorage<K, T> : IStateStorage<K, T> where T : class, IState<K>
    {
        readonly StorageConfig grainConfig;
        public MongoStateStorage(StorageConfig grainConfig)
        {
            this.grainConfig = grainConfig;
        }
        public async Task DeleteAsync(K id)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("StateId", id);
            await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).DeleteManyAsync(filter);
        }
        public async Task<T> GetByIdAsync(K id)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("StateId", id);
            var cursor = await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).FindAsync<BsonDocument>(filter);
            var document = await cursor.FirstOrDefaultAsync();
            T result = null;
            if (document != null)
            {
                var data = document["Data"]?.AsByteArray;
                if (data != null)
                {
                    using (var ms = new MemoryStream(data))
                    {
                        result = Serializer.Deserialize<T>(ms);
                    }
                }
            }
            return result;
        }

        public async Task InsertAsync(T data)
        {
            var mState = new MongoState<K>
            {
                StateId = data.StateId,
                Id = ObjectId.GenerateNewId().ToString(),
                Version = data.Version
            };
            using (var ms = new PooledMemoryStream())
            {
                Serializer.Serialize<T>(ms, data);
                mState.Data = ms.ToArray();
            }
            if (mState.Data != null && mState.Data.Count() > 0)
                await grainConfig.Storage.GetCollection<MongoState<K>>(grainConfig.DataBase, grainConfig.SnapshotCollection).InsertOneAsync(mState, null, new CancellationTokenSource(3000).Token);
        }

        public async Task UpdateAsync(T data)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("StateId", data.StateId);
            byte[] bytes;
            using (var ms = new PooledMemoryStream())
            {
                Serializer.Serialize<T>(ms, data);
                bytes = ms.ToArray();
            }
            if (bytes != null && bytes.Count() > 0)
            {
                var update = Builders<BsonDocument>.Update.Set("Data", bytes).Set("Version", data.Version);
                await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotCollection).UpdateOneAsync(filter, update, null, new CancellationTokenSource(3000).Token);
            }
        }
    }
}
