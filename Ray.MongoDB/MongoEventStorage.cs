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
using System.Threading.Tasks.Dataflow;

namespace Ray.MongoDB
{
    public class MongoEventStorage<K> : IEventStorage<K>
    {
        readonly MongoGrainConfig grainConfig;
        readonly BufferBlock<EventBytesTransactionWrap<K>> EventSaveFlowChannel = new BufferBlock<EventBytesTransactionWrap<K>>();
        int isProcessing = 0;
        public MongoEventStorage(MongoGrainConfig grainConfig)
        {
            this.grainConfig = grainConfig;
        }
        public async Task<IList<IEventBase<K>>> GetListAsync(K stateId, Int64 startVersion, Int64 endVersion, DateTime? startTime = null)
        {
            var collectionListTask = grainConfig.GetCollectionList(startTime);
            if (!collectionListTask.IsCompleted)
                await collectionListTask;
            var list = new List<IEventBase<K>>();
            long readVersion = 0;
            foreach (var collection in collectionListTask.Result)
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Lte("Version", endVersion) & filterBuilder.Gt("Version", startVersion);
                var cursor = await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, collection.Name).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(10000).Token);
                foreach (var document in cursor.ToEnumerable())
                {
                    var typeCode = document["TypeCode"].AsString;
                    if (MessageTypeMapper.TryGetValue(typeCode, out var type))
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
            var collectionListTask = grainConfig.GetCollectionList(startTime);
            if (!collectionListTask.IsCompleted)
                await collectionListTask;
            var list = new List<IEventBase<K>>();
            foreach (var collection in collectionListTask.Result)
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Eq("TypeCode", typeCode) & filterBuilder.Gt("Version", startVersion);
                var cursor = await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, collection.Name).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(10000).Token);
                foreach (var document in cursor.ToEnumerable())
                {
                    if (MessageTypeMapper.TryGetValue(typeCode, out var type))
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
                if (!EventSaveFlowChannel.Post(wrap))
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
                    TypeCode = evt.Value.GetType().FullName,
                    Data = evt.Bytes,
                    UniqueId = string.IsNullOrEmpty(evt.UniqueId) ? evt.Value.Version.ToString() : evt.UniqueId
                });
                if ((DateTime.UtcNow - start).TotalMilliseconds > 100) break;//保证批量延时不超过100ms
            }
            var collectionTask = grainConfig.GetCollection(DateTime.UtcNow);
            if (!collectionTask.IsCompleted)
                await collectionTask;
            var collection = grainConfig.Storage.GetCollection<MongoEvent<K>>(grainConfig.DataBase, collectionTask.Result.Name);
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
                            TypeCode = w.Value.GetType().FullName,
                            Data = w.Bytes,
                            UniqueId = string.IsNullOrEmpty(w.UniqueId) ? w.Value.Version.ToString() : w.UniqueId
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

        public async Task TransactionSaveAsync(List<EventSaveWrap<K>> list)
        {
            var inserts = new List<MongoEvent<K>>();
            foreach (var data in list)
            {
                var mEvent = new MongoEvent<K>
                {
                    Id = new ObjectId(),
                    StateId = data.Evt.StateId,
                    Version = data.Evt.Version,
                    TypeCode = data.Evt.GetType().FullName,
                    Data = data.Bytes,
                    UniqueId = string.IsNullOrEmpty(data.UniqueId) ? data.Evt.Version.ToString() : data.UniqueId
                };
                inserts.Add(mEvent);
            }
            var collectionTask = grainConfig.GetCollection(DateTime.UtcNow);
            if (!collectionTask.IsCompleted)
                await collectionTask;
            await grainConfig.Storage.GetCollection<MongoEvent<K>>(grainConfig.DataBase, collectionTask.Result.Name).InsertManyAsync(inserts);
        }
    }
}
