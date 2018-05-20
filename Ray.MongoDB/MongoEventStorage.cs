using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using ProtoBuf;
using MongoDB.Driver;
using MongoDB.Bson;
using System;
using System.Threading;
using Ray.Core.EventSourcing;
using Ray.Core.Message;
using Microsoft.Extensions.Logging;
using Ray.Core.Utils;

namespace Ray.MongoDB
{
    public class MongoEventStorage<K> : IEventStorage<K>
    {
        MongoGrainConfig grainConfig;
        ILogger<MongoEventStorage<K>> logger;
        IMongoStorage mongoStorage;
        public MongoEventStorage(IMongoStorage mongoStorage, ILogger<MongoEventStorage<K>> logger, MongoGrainConfig grainConfig)
        {
            this.mongoStorage = mongoStorage;
            this.grainConfig = grainConfig;
            this.logger = logger;
        }
        public async Task<IList<IEventBase<K>>> GetListAsync(K stateId, Int64 startVersion, Int64 endVersion, DateTime? startTime = null)
        {
            var collectionList = grainConfig.GetCollectionList(mongoStorage, mongoStorage.Config.SysStartTime, startTime);
            var list = new List<IEventBase<K>>();
            Int64 readVersion = 0;
            foreach (var collection in collectionList)
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Lte("Version", endVersion) & filterBuilder.Gt("Version", startVersion);
                var cursor = await mongoStorage.GetCollection<BsonDocument>(grainConfig.EventDataBase, collection.Name).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(3000).Token);
                foreach (var document in cursor.ToEnumerable())
                {
                    var typeCode = document["TypeCode"].AsString;
                    var type = MessageTypeMapper.GetType(typeCode);
                    var data = document["Data"].AsByteArray;
                    using (var ms = new MemoryStream(data))
                    {
                        if (Serializer.Deserialize(type, ms) is IEventBase<K> evt)
                        {
                            readVersion = evt.Version;
                            if (readVersion <= endVersion)
                                list.Add(evt);
                        }
                    }
                }
                if (readVersion >= endVersion)
                    break;
            }
            return list;
        }
        public async Task<IList<IEventBase<K>>> GetListAsync(K stateId, string typeCode, Int64 startVersion, Int64 endVersion, DateTime? startTime = null)
        {
            var collectionList = grainConfig.GetCollectionList(mongoStorage, mongoStorage.Config.SysStartTime, startTime);
            var list = new List<IEventBase<K>>();
            Int64 readVersion = 0;
            foreach (var collection in collectionList)
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Eq("TypeCode", typeCode) & filterBuilder.Gt("Version", startVersion);
                var cursor = await mongoStorage.GetCollection<BsonDocument>(grainConfig.EventDataBase, collection.Name).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(3000).Token);
                foreach (var document in cursor.ToEnumerable())
                {
                    var type = MessageTypeMapper.GetType(typeCode);
                    var data = document["Data"].AsByteArray;
                    using (var ms = new MemoryStream(data))
                    {
                        if (readVersion <= endVersion)
                        {
                            if (Serializer.Deserialize(type, ms) is IEventBase<K> evt)
                            {
                                readVersion = evt.Version;
                                if (readVersion <= endVersion)
                                    list.Add(evt);
                            }
                        }
                    }

                }
                if (readVersion >= endVersion)
                    break;
            }
            return list;
        }
        public async Task<bool> SaveAsync(IEventBase<K> data, byte[] bytes, string uniqueId = null)
        {
            var mEvent = new MongoEvent<K>
            {
                Id = OGuid.GenerateNewId().ToString(),
                StateId = data.StateId,
                Version = data.Version,
                TypeCode = data.TypeCode,
                Data = bytes,
                UniqueId = string.IsNullOrEmpty(uniqueId) ? data.GetUniqueId() : uniqueId
            };
            try
            {
                await mongoStorage.GetCollection<MongoEvent<K>>(grainConfig.EventDataBase, grainConfig.GetCollection(mongoStorage, mongoStorage.Config.SysStartTime, data.Timestamp).Name).InsertOneAsync(mEvent);
                return true;
            }
            catch (MongoWriteException ex)
            {
                if (ex.WriteError.Category != ServerErrorCategory.DuplicateKey)
                {
                    throw ex;
                }
                else
                {
                    logger.LogError(ex, $"Event Duplicate,Event:{Newtonsoft.Json.JsonConvert.SerializeObject(data)}");
                }
            }
            return false;
        }
    }
}
