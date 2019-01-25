using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Channels;
using Ray.Core.Event;
using Ray.Core.Serialization;
using Ray.Core.Storage;

namespace Ray.Storage.MongoDB
{
    public class MongoEventStorage<K, E> : IEventStorage<K, E>
        where E : IEventBase<K>
    {
        readonly StorageConfig grainConfig;
        readonly IMpscChannel<DataAsyncWrapper<EventSaveWrapper<K, E>, bool>> mpscChannel;
        readonly ILogger<MongoEventStorage<K, E>> logger;
        readonly ISerializer serializer;
        public MongoEventStorage(IServiceProvider serviceProvider, StorageConfig grainConfig)
        {
            serializer = serviceProvider.GetService<ISerializer>();
            logger = serviceProvider.GetService<ILogger<MongoEventStorage<K, E>>>();
            mpscChannel = serviceProvider.GetService<IMpscChannel<DataAsyncWrapper<EventSaveWrapper<K, E>, bool>>>();
            mpscChannel.BindConsumer(BatchProcessing).ActiveConsumer();
            this.grainConfig = grainConfig;
        }
        public async Task<IList<IEvent<K, E>>> GetList(K stateId, long latestTimestamp, long startVersion, long endVersion)
        {
            var collectionListTask = grainConfig.GetCollectionList();
            if (!collectionListTask.IsCompleted)
                await collectionListTask;
            var list = new List<IEvent<K, E>>();
            long readVersion = 0;
            foreach (var collection in collectionListTask.Result.Where(c => c.CreateTime >= latestTimestamp))
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Lte("Version", endVersion) & filterBuilder.Gt("Version", startVersion);
                var cursor = await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, collection.Name).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(10000).Token);
                foreach (var document in cursor.ToEnumerable())
                {
                    var typeCode = document["TypeCode"].AsString;
                    var data = document["Data"].AsByteArray;
                    using (var ms = new MemoryStream(data))
                    {
                        if (serializer.Deserialize(TypeContainer.GetType(typeCode), ms) is IEvent<K, E> evt)
                        {
                            readVersion = evt.Base.Version;
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
        public async Task<IList<IEvent<K, E>>> GetListByType(K stateId, string typeCode, long startVersion, int limit)
        {
            var collectionListTask = grainConfig.GetCollectionList();
            if (!collectionListTask.IsCompleted)
                await collectionListTask;
            var list = new List<IEvent<K, E>>();
            foreach (var collection in collectionListTask.Result)
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Eq("TypeCode", typeCode) & filterBuilder.Gt("Version", startVersion);
                var cursor = await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, collection.Name).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(10000).Token);
                foreach (var document in cursor.ToEnumerable())
                {
                    var data = document["Data"].AsByteArray;
                    using (var ms = new MemoryStream(data))
                    {
                        if (serializer.Deserialize(TypeContainer.GetType(typeCode), ms) is IEvent<K, E> evt)
                        {
                            list.Add(evt);
                        }
                    }
                }
                if (list.Count >= limit)
                    break;
            }
            return list;
        }
        public Task<bool> Append(IEvent<K, E> evt, byte[] bytes, string uniqueId = null)
        {
            return Task.Run(async () =>
            {
                var wrap = new DataAsyncWrapper<EventSaveWrapper<K, E>, bool>(new EventSaveWrapper<K, E>(evt, bytes, uniqueId));
                var writeTask = mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompleted)
                    await writeTask;
                return await wrap.TaskSource.Task;
            });
        }
        private async Task BatchProcessing(List<DataAsyncWrapper<EventSaveWrapper<K, E>, bool>> wrapperList)
        {
            var documents = new List<MongoEvent<K>>();
            foreach (var wrap in wrapperList)
            {
                documents.Add(new MongoEvent<K>
                {
                    Id = new ObjectId(),
                    StateId = wrap.Value.Event.Base.StateId,
                    Version = wrap.Value.Event.Base.Version,
                    TypeCode = wrap.Value.GetType().FullName,
                    Data = wrap.Value.Bytes,
                    UniqueId = string.IsNullOrEmpty(wrap.Value.UniqueId) ? wrap.Value.Event.Base.Version.ToString() : wrap.Value.UniqueId
                });
            }
            var collectionTask = grainConfig.GetCollection(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            if (!collectionTask.IsCompleted)
                await collectionTask;
            var collection = grainConfig.Storage.GetCollection<MongoEvent<K>>(grainConfig.DataBase, collectionTask.Result.Name);
            wrapperList.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
            try
            {
                await collection.InsertManyAsync(documents);
                wrapperList.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
            }
            catch
            {
                foreach (var wrapper in wrapperList)
                {
                    try
                    {
                        await collection.InsertOneAsync(new MongoEvent<K>
                        {
                            Id = new ObjectId(),
                            StateId = wrapper.Value.Event.Base.StateId,
                            Version = wrapper.Value.Event.Base.Version,
                            TypeCode = wrapper.Value.GetType().FullName,
                            Data = wrapper.Value.Bytes,
                            UniqueId = string.IsNullOrEmpty(wrapper.Value.UniqueId) ? wrapper.Value.Event.Base.Version.ToString() : wrapper.Value.UniqueId
                        });
                        wrapper.TaskSource.TrySetResult(true);
                    }
                    catch (MongoWriteException ex)
                    {
                        if (ex.WriteError.Category != ServerErrorCategory.DuplicateKey)
                        {
                            wrapper.TaskSource.TrySetException(ex);
                        }
                        else
                        {
                            wrapper.TaskSource.TrySetResult(false);
                        }
                    }
                }
            }
        }

        public async Task TransactionBatchAppend(List<EventTransmitWrapper<K, E>> list)
        {
            var inserts = new List<MongoEvent<K>>();
            foreach (var data in list)
            {
                var mEvent = new MongoEvent<K>
                {
                    Id = new ObjectId(),
                    StateId = data.Evt.Base.StateId,
                    Version = data.Evt.Base.Version,
                    TypeCode = data.Evt.GetType().FullName,
                    Data = data.Bytes,
                    UniqueId = string.IsNullOrEmpty(data.UniqueId) ? data.Evt.Base.Version.ToString() : data.UniqueId
                };
                inserts.Add(mEvent);
            }
            var collectionTask = grainConfig.GetCollection(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            if (!collectionTask.IsCompleted)
                await collectionTask;
            await grainConfig.Storage.GetCollection<MongoEvent<K>>(grainConfig.DataBase, collectionTask.Result.Name).InsertManyAsync(inserts);
        }

        public Task Delete(K stateId, long endVersion)
        {
            //TODO 实现Delete
            throw new NotImplementedException();
        }
    }
}
