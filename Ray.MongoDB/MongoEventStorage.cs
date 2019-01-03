using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using ProtoBuf;
using Ray.Core.Abstractions;
using Ray.Core.Internal;
using Ray.Core.Messaging;

namespace Ray.MongoDB
{
    public class MongoEventStorage<K> : IEventStorage<K>
    {
        readonly MongoGrainConfig grainConfig;
        readonly IMpscChannel<BytesEventTaskSource<K>> mpscChannel;
        readonly ILogger<MongoEventStorage<K>> logger;
        public MongoEventStorage(IServiceProvider serviceProvider, MongoGrainConfig grainConfig)
        {
            logger = serviceProvider.GetService<ILogger<MongoEventStorage<K>>>();
            mpscChannel = serviceProvider.GetService<IMpscChannel<BytesEventTaskSource<K>>>();
            mpscChannel.BindConsumer(BatchProcessing).ActiveConsumer();
            this.grainConfig = grainConfig;
        }
        public async Task<IList<IEventBase<K>>> GetListAsync(K stateId, long startVersion, long endVersion)
        {
            var collectionListTask = grainConfig.GetCollectionList();
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
                    var data = document["Data"].AsByteArray;
                    using (var ms = new MemoryStream(data))
                    {
                        if (Serializer.Deserialize(TypeContainer.GetType(typeCode), ms) is IEventBase<K> evt)
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
        public async Task<IList<IEventBase<K>>> GetListAsync(K stateId, string typeCode, long startVersion, int limit)
        {
            var collectionListTask = grainConfig.GetCollectionList();
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
                    var data = document["Data"].AsByteArray;
                    using (var ms = new MemoryStream(data))
                    {
                        if (Serializer.Deserialize(TypeContainer.GetType(typeCode), ms) is IEventBase<K> evt)
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
        public Task<bool> SaveAsync(IEventBase<K> evt, byte[] bytes, string uniqueId = null)
        {
            return Task.Run(async () =>
            {
                var wrap = new BytesEventTaskSource<K>(evt, bytes, uniqueId);
                var writeTask = mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompleted)
                    await writeTask;
                return await wrap.TaskSource.Task;
            });
        }
        private async Task BatchProcessing(List<BytesEventTaskSource<K>> wrapList)
        {
            var documents = new List<MongoEvent<K>>();
            foreach (var wrap in wrapList)
            {
                documents.Add(new MongoEvent<K>
                {
                    Id = new ObjectId(),
                    StateId = wrap.Value.StateId,
                    Version = wrap.Value.Version,
                    TypeCode = wrap.Value.GetType().FullName,
                    Data = wrap.Bytes,
                    UniqueId = string.IsNullOrEmpty(wrap.UniqueId) ? wrap.Value.Version.ToString() : wrap.UniqueId
                });
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
                foreach (var wrap in wrapList)
                {
                    try
                    {
                        await collection.InsertOneAsync(new MongoEvent<K>
                        {
                            Id = new ObjectId(),
                            StateId = wrap.Value.StateId,
                            Version = wrap.Value.Version,
                            TypeCode = wrap.Value.GetType().FullName,
                            Data = wrap.Bytes,
                            UniqueId = string.IsNullOrEmpty(wrap.UniqueId) ? wrap.Value.Version.ToString() : wrap.UniqueId
                        });
                        wrap.TaskSource.TrySetResult(true);
                    }
                    catch (MongoWriteException ex)
                    {
                        if (ex.WriteError.Category != ServerErrorCategory.DuplicateKey)
                        {
                            wrap.TaskSource.TrySetException(ex);
                        }
                        else
                        {
                            wrap.TaskSource.TrySetResult(false);
                        }
                    }
                }
            }
        }

        public async Task TransactionSaveAsync(List<TransactionEventWrapper<K>> list)
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
