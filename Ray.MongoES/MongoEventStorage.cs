using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using ProtoBuf;
using MongoDB.Driver;
using MongoDB.Bson;
using System;
using System.Linq;
using System.Threading;
using Ray.Core.EventSourcing;
using Ray.Core.Message;
using Microsoft.Extensions.Logging;

namespace Ray.MongoES
{
    public class MongoEventStorage<K> : IEventStorage<K>
    {
        MongoStorageAttribute mongoAttr;
        ILogger<MongoEventStorage<K>> logger;
        IMongoStorage mongoStorage;
        public MongoEventStorage(IMongoStorage mongoStorage, ILogger<MongoEventStorage<K>> logger, MongoStorageAttribute mongoAttr)
        {
            this.mongoStorage = mongoStorage;
            this.mongoAttr = mongoAttr;
            this.logger = logger;
        }
        public async Task<List<EventInfo<K>>> GetListAsync(K stateId, Int64 startVersion, Int64 endVersion, DateTime? startTime = null)
        {
            var collectionList = mongoAttr.GetCollectionList(mongoStorage, mongoStorage.Config.SysStartTime, startTime);
            var list = new List<EventInfo<K>>();
            Int64 readVersion = 0;
            foreach (var collection in collectionList)
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Lte("Version", endVersion) & filterBuilder.Gt("Version", startVersion);
                var cursor = await mongoStorage.GetCollection<BsonDocument>(mongoAttr.EventDataBase, collection.Name).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(3000).Token);
                foreach (var document in cursor.ToEnumerable())
                {
                    var typeCode = document["TypeCode"].AsString;
                    var type = MessageTypeMapper.GetType(typeCode);
                    var data = document["Data"].AsByteArray;
                    var eventInfo = new EventInfo<K>();
                    eventInfo.IsComplete = document["IsComplete"].AsBoolean;
                    using (var ms = new MemoryStream(data))
                    {
                        var @event = Serializer.Deserialize(type, ms) as IEventBase<K>;
                        readVersion = @event.Version;
                        eventInfo.Event = @event;
                    }
                    if (readVersion <= endVersion)
                        list.Add(eventInfo);
                }
                if (readVersion >= endVersion)
                    break;
            }
            return list.OrderBy(e => e.Event.Version).ToList();
        }
        public async Task<List<EventInfo<K>>> GetListAsync(K stateId, string typeCode, Int64 startVersion, Int64 endVersion, DateTime? startTime = null)
        {
            var collectionList = mongoAttr.GetCollectionList(mongoStorage, mongoStorage.Config.SysStartTime, startTime);
            var list = new List<EventInfo<K>>();
            Int64 readVersion = 0;
            foreach (var collection in collectionList)
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Eq("TypeCode", typeCode) & filterBuilder.Gt("Version", startVersion);
                var cursor = await mongoStorage.GetCollection<BsonDocument>(mongoAttr.EventDataBase, collection.Name).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(3000).Token);
                foreach (var document in cursor.ToEnumerable())
                {
                    var type = MessageTypeMapper.GetType(typeCode);
                    var data = document["Data"].AsByteArray;
                    var eventInfo = new EventInfo<K>();
                    eventInfo.IsComplete = document["IsComplete"].AsBoolean;
                    using (MemoryStream ms = new MemoryStream(data))
                    {
                        var @event = Serializer.Deserialize(type, ms) as IEventBase<K>;
                        eventInfo.Event = @event;
                    }
                    if (readVersion <= endVersion)
                        list.Add(eventInfo);
                }
                if (readVersion >= endVersion)
                    break;
            }
            return list.OrderBy(e => e.Event.Version).ToList();
        }
        public async Task<bool> SaveAsync<T>(T data, byte[] bytes, string uniqueId = null) where T : IEventBase<K>
        {
            var mEvent = new MongoEvent<K>();
            mEvent.StateId = data.StateId;
            mEvent.Version = data.Version;
            mEvent.TypeCode = data.TypeCode;
            mEvent.Data = bytes;
            if (string.IsNullOrEmpty(data.Id))
            {
                mEvent.Id = ObjectId.GenerateNewId().ToString();
                data.Id = mEvent.Id;
            }
            else
            {
                mEvent.Id = data.Id;
            }

            if (string.IsNullOrEmpty(uniqueId))
                mEvent.MsgId = mEvent.Id;
            else
                mEvent.MsgId = uniqueId;
            try
            {
                await mongoStorage.GetCollection<MongoEvent<K>>(mongoAttr.EventDataBase, mongoAttr.GetCollection(mongoStorage, mongoStorage.Config.SysStartTime, data.Timestamp).Name).InsertOneAsync(mEvent);
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
                    logger.LogError(ex, $"事件重复插入,Event:{Newtonsoft.Json.JsonConvert.SerializeObject(data)}");
                }
            }
            return false;
        }

        public async Task CompleteAsync<T>(T data) where T : IEventBase<K>
        {
            var filter = Builders<BsonDocument>.Filter.Eq("_id", data.Id);
            var update = Builders<BsonDocument>.Update.Set("IsComplete", true);
            await mongoStorage.GetCollection<BsonDocument>(mongoAttr.EventDataBase, mongoAttr.GetCollection(mongoStorage, mongoStorage.Config.SysStartTime, data.Timestamp).Name).UpdateOneAsync(filter, update);
        }
    }
}
