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
using System.Threading.Tasks.Dataflow;
using System.Linq;

namespace Ray.MongoDB
{
    public class MongoEventStorage<K> : IEventStorage<K>
    {
        MongoGrainConfig grainConfig;
        ILogger<MongoEventStorage<K>> logger;
        IMongoStorage mongoStorage;
        BufferBlock<EventBytesTransactionWrap<K>> EventSaveFlowChannel = new BufferBlock<EventBytesTransactionWrap<K>>();
        int isProcessing = 0;
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
            long readVersion = 0;
            foreach (var collection in collectionList)
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Lte("Version", endVersion) & filterBuilder.Gt("Version", startVersion);
                var cursor = await mongoStorage.GetCollection<BsonDocument>(grainConfig.EventDataBase, collection.Name).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(10000).Token);
                foreach (var document in cursor.ToEnumerable())
                {
                    var typeCode = document["TypeCode"].AsString;
                    if (MessageTypeMapper.EventTypeDict.TryGetValue(typeCode, out var type))
                    {
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
                }
                if (readVersion >= endVersion)
                    break;
            }
            return list;
        }
        public async Task<IList<IEventBase<K>>> GetListAsync(K stateId, string typeCode, Int64 startVersion, Int32 limit, DateTime? startTime = null)
        {
            var collectionList = grainConfig.GetCollectionList(mongoStorage, mongoStorage.Config.SysStartTime, startTime);
            var list = new List<IEventBase<K>>();
            foreach (var collection in collectionList)
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Eq("TypeCode", typeCode) & filterBuilder.Gt("Version", startVersion);
                var cursor = await mongoStorage.GetCollection<BsonDocument>(grainConfig.EventDataBase, collection.Name).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(10000).Token);
                foreach (var document in cursor.ToEnumerable())
                {
                    if (MessageTypeMapper.EventTypeDict.TryGetValue(typeCode, out var type))
                    {
                        var data = document["Data"].AsByteArray;
                        using (var ms = new MemoryStream(data))
                        {
                            if (Serializer.Deserialize(type, ms) is IEventBase<K> evt)
                            {
                                list.Add(evt);
                            }
                        }
                    }
                }
                if (list.Count >= limit)
                    break;
            }
            return list;
        }
        public Task<bool> SaveAsync(IEventBase<K> evt, byte[] bytes, string uniqueId = null)
        {
            return Task.Run(async () =>
            {
                var wrap = EventBytesTransactionWrap<K>.Create(evt, bytes, uniqueId);
                await EventSaveFlowChannel.SendAsync(wrap);
                if (isProcessing == 0)
                    TriggerFlowProcess();
                return await wrap.TaskSource.Task;
            });
        }
        private async void TriggerFlowProcess()
        {
            await Task.Run(async () =>
            {
                if (Interlocked.CompareExchange(ref isProcessing, 1, 0) == 0)
                {
                    try
                    {
                        while (await EventSaveFlowChannel.OutputAvailableAsync())
                        {
                            await FlowProcess();
                        }
                    }
                    finally
                    {
                        Interlocked.Exchange(ref isProcessing, 0);
                    }
                }
            }).ConfigureAwait(false);

        }
        private async ValueTask FlowProcess()
        {
            var start = DateTime.UtcNow;
            var wrapList = new List<EventBytesTransactionWrap<K>>();
            var documents = new List<MongoEvent<K>>();
            while (EventSaveFlowChannel.TryReceive(out var evt))
            {
                wrapList.Add(evt);
                documents.Add(new MongoEvent<K>
                {
                    Id = new ObjectId(),
                    StateId = evt.Value.StateId,
                    Version = evt.Value.Version,
                    TypeCode = evt.Value.TypeCode,
                    Data = evt.Bytes,
                    UniqueId = evt.UniqueId
                });
                if ((DateTime.UtcNow - start).TotalMilliseconds > 100) break;//保证批量延时不超过100ms
            }
            var collection = mongoStorage.GetCollection<MongoEvent<K>>(grainConfig.EventDataBase, grainConfig.GetCollection(mongoStorage, mongoStorage.Config.SysStartTime, DateTime.UtcNow).Name);
            wrapList.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
            try
            {
                await collection.InsertManyAsync(documents);
                wrapList.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
            }
            catch
            {
                foreach (var w in wrapList)
                {
                    try
                    {
                        await collection.InsertOneAsync(new MongoEvent<K>
                        {
                            Id = new ObjectId(),
                            StateId = w.Value.StateId,
                            Version = w.Value.Version,
                            TypeCode = w.Value.TypeCode,
                            Data = w.Bytes,
                            UniqueId = w.UniqueId
                        });
                        w.TaskSource.TrySetResult(true);
                    }
                    catch (MongoWriteException ex)
                    {
                        if (ex.WriteError.Category != ServerErrorCategory.DuplicateKey)
                        {
                            w.TaskSource.TrySetException(ex);
                        }
                        else
                        {
                            w.TaskSource.TrySetResult(false);
                        }
                    }
                }
            }
        }

        public async ValueTask TransactionSaveAsync(List<EventSaveWrap<K>> list)
        {
            var inserts = new List<MongoEvent<K>>();
            foreach (var data in list)
            {
                var mEvent = new MongoEvent<K>
                {
                    Id = new ObjectId(),
                    StateId = data.Evt.StateId,
                    Version = data.Evt.Version,
                    TypeCode = data.Evt.TypeCode,
                    Data = data.Bytes,
                    UniqueId = data.UniqueId
                };
                inserts.Add(mEvent);
            }
            await mongoStorage.GetCollection<MongoEvent<K>>(grainConfig.EventDataBase, grainConfig.GetCollection(mongoStorage, mongoStorage.Config.SysStartTime, DateTime.UtcNow).Name).InsertManyAsync(inserts);
        }
    }
}
