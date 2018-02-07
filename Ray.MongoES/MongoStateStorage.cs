using System.Linq;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ProtoBuf;
using MongoDB.Driver;
using MongoDB.Bson;
using Ray.Core.EventSourcing;
using System;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.MongoES
{
    public class MongoStateStorage<T, K> : MongoStorage, IStateStorage<T, K> where T : class, IState<K>
    {
        string database, collection;
        public MongoStateStorage(string database, string collection, IServiceProvider svProvider) : base(svProvider.GetService<IOptions<MongoConfig>>())
        {
            this.database = database;
            this.collection = collection;
        }
        public async Task DeleteAsync(K id)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("StateId", id);
            await GetCollection<BsonDocument>(database, collection).DeleteManyAsync(filter);
        }
        public async Task<T> GetByIdAsync(K id)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("StateId", id);
            var cursor = await GetCollection<BsonDocument>(database, collection).FindAsync<BsonDocument>(filter);
            var document = await cursor.FirstOrDefaultAsync();
            T result = null;
            if (document != null)
            {
                var data = document["Data"]?.AsByteArray;
                if (data != null)
                {
                    using (MemoryStream ms = new MemoryStream(data))
                    {
                        result = Serializer.Deserialize<T>(ms);
                    }
                }
            }
            return result;
        }

        public async Task InsertAsync(T data)
        {
            var mState = new MongoState<K>();
            mState.StateId = data.StateId;
            mState.Id = ObjectId.GenerateNewId().ToString();
            using (MemoryStream ms = new MemoryStream())
            {
                Serializer.Serialize<T>(ms, data);
                mState.Data = ms.ToArray();
            }
            if (mState.Data != null && mState.Data.Count() > 0)
                await GetCollection<MongoState<K>>(database, collection).InsertOneAsync(mState, null, new CancellationTokenSource(3000).Token);
        }

        public async Task UpdateAsync(T data)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("StateId", data.StateId);
            byte[] bytes;
            using (MemoryStream ms = new MemoryStream())
            {
                Serializer.Serialize<T>(ms, data);
                bytes = ms.ToArray();
            }
            if (bytes != null && bytes.Count() > 0)
            {
                var update = Builders<BsonDocument>.Update.Set("Data", bytes);
                await GetCollection<BsonDocument>(database, collection).UpdateOneAsync(filter, update, null, new CancellationTokenSource(3000).Token);
            }
        }
    }
}
