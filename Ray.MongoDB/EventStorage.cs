using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
    public class EventStorage<PrimaryKey> : IEventStorage<PrimaryKey>
    {
        readonly StorageConfig grainConfig;
        readonly IMpscChannel<DataAsyncWrapper<SaveTransport<PrimaryKey>, bool>> mpscChannel;
        readonly ILogger<EventStorage<PrimaryKey>> logger;
        readonly ISerializer serializer;
        public EventStorage(IServiceProvider serviceProvider, StorageConfig grainConfig)
        {
            serializer = serviceProvider.GetService<ISerializer>();
            logger = serviceProvider.GetService<ILogger<EventStorage<PrimaryKey>>>();
            mpscChannel = serviceProvider.GetService<IMpscChannel<DataAsyncWrapper<SaveTransport<PrimaryKey>, bool>>>();
            mpscChannel.BindConsumer(BatchProcessing).ActiveConsumer();
            this.grainConfig = grainConfig;
        }
        public async Task<IList<IFullyEvent<PrimaryKey>>> GetList(PrimaryKey stateId, long latestTimestamp, long startVersion, long endVersion)
        {
            var collectionListTask = grainConfig.GetCollectionList();
            if (!collectionListTask.IsCompletedSuccessfully)
                await collectionListTask;
            var list = new List<IFullyEvent<PrimaryKey>>();
            long readVersion = 0;
            foreach (var collection in collectionListTask.Result.Where(c => c.CreateTime >= latestTimestamp))
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Lte("Version", endVersion) & filterBuilder.Gt("Version", startVersion);
                var cursor = await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, collection.Name).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(10000).Token);
                foreach (var document in cursor.ToEnumerable())
                {
                    var typeCode = document["TypeCode"].AsString;
                    var data = document["Data"].AsString;
                    var timestamp = document["Timestamp"].AsInt64;
                    readVersion = document["Version"].AsInt64;
                    if (readVersion <= endVersion)
                    {
                        if (serializer.Deserialize(TypeContainer.GetType(typeCode), Encoding.Default.GetBytes(data)) is IEvent evt)
                        {
                            if (typeof(PrimaryKey) == typeof(long) && document["StateId"].AsInt64 is PrimaryKey actorIdWithLong)
                            {
                                list.Add(new FullyEvent<PrimaryKey>
                                {
                                    StateId = actorIdWithLong,
                                    Event = evt,
                                    Base = new EventBase(readVersion, timestamp)
                                });
                            }
                            else if (document["StateId"].AsString is PrimaryKey actorIdWithString)
                            {
                                list.Add(new FullyEvent<PrimaryKey>
                                {
                                    StateId = actorIdWithString,
                                    Event = evt,
                                    Base = new EventBase(readVersion, timestamp)
                                });
                            }
                        }
                    }
                }
                if (readVersion >= endVersion)
                    break;
            }
            return list;
        }
        public async Task<IList<IFullyEvent<PrimaryKey>>> GetListByType(PrimaryKey stateId, string typeCode, long startVersion, int limit)
        {
            var collectionListTask = grainConfig.GetCollectionList();
            if (!collectionListTask.IsCompletedSuccessfully)
                await collectionListTask;
            var list = new List<IFullyEvent<PrimaryKey>>();
            foreach (var collection in collectionListTask.Result)
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Eq("TypeCode", typeCode) & filterBuilder.Gt("Version", startVersion);
                var cursor = await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, collection.Name).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(10000).Token);
                foreach (var document in cursor.ToEnumerable())
                {
                    var data = document["Data"].AsString;
                    var timestamp = document["Timestamp"].AsInt64;
                    var version = document["Version"].AsInt64;
                    if (serializer.Deserialize(TypeContainer.GetType(typeCode), Encoding.Default.GetBytes(data)) is IEvent evt)
                    {
                        if (typeof(PrimaryKey) == typeof(long) && document["StateId"].AsInt64 is PrimaryKey actorIdWithLong)
                        {
                            list.Add(new FullyEvent<PrimaryKey>
                            {
                                StateId = actorIdWithLong,
                                Event = evt,
                                Base = new EventBase(version, timestamp)
                            });
                        }
                        else if (document["StateId"].AsString is PrimaryKey actorIdWithString)
                        {
                            list.Add(new FullyEvent<PrimaryKey>
                            {
                                StateId = actorIdWithString,
                                Event = evt,
                                Base = new EventBase(version, timestamp)
                            });
                        }
                    }
                }
                if (list.Count >= limit)
                    break;
            }
            return list;
        }
        public Task<bool> Append(SaveTransport<PrimaryKey> saveTransport)
        {
            return Task.Run(async () =>
            {
                var wrap = new DataAsyncWrapper<SaveTransport<PrimaryKey>, bool>(saveTransport);
                var writeTask = mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompletedSuccessfully)
                    await writeTask;
                return await wrap.TaskSource.Task;
            });
        }
        private async Task BatchProcessing(List<DataAsyncWrapper<SaveTransport<PrimaryKey>, bool>> wrapperList)
        {
            var documents = new List<BsonDocument>();
            foreach (var wrapper in wrapperList)
            {
                documents.Add(new BsonDocument
                {
                    {"StateId",BsonValue.Create( wrapper.Value.Event.StateId) },
                    {"Version",wrapper.Value.Event.Base.Version },
                    {"Timestamp",wrapper.Value.Event.Base.Timestamp },
                    {"TypeCode",wrapper.Value.Event.Event.GetType().FullName },
                    {"Data",Encoding.Default.GetString(wrapper.Value.BytesTransport.EventBytes)},
                    {"UniqueId",string.IsNullOrEmpty(wrapper.Value.UniqueId) ? wrapper.Value.Event.Base.Version.ToString() : wrapper.Value.UniqueId }
                });
            }
            var collectionTask = grainConfig.GetCollection(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            if (!collectionTask.IsCompletedSuccessfully)
                await collectionTask;
            var collection = grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, collectionTask.Result.Name);
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
                        await collection.InsertOneAsync(new BsonDocument
                        {
                            {"StateId",BsonValue.Create( wrapper.Value.Event.StateId) },
                            {"Version",wrapper.Value.Event.Base.Version },
                            {"Timestamp",wrapper.Value.Event.Base.Timestamp },
                            {"TypeCode",wrapper.Value.Event.Event.GetType().FullName },
                            {"Data",Encoding.Default.GetString(wrapper.Value.BytesTransport.EventBytes)},
                            {"UniqueId",string.IsNullOrEmpty(wrapper.Value.UniqueId) ? wrapper.Value.Event.Base.Version.ToString() : wrapper.Value.UniqueId }
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

        public async Task TransactionBatchAppend(List<TransactionTransport<PrimaryKey>> list)
        {
            var documents = new List<BsonDocument>();
            foreach (var data in list)
            {
                documents.Add(new BsonDocument
                {
                    {"StateId",BsonValue.Create( data.FullyEvent.StateId) },
                    {"Version", data.FullyEvent.Base.Version },
                    {"Timestamp",data.FullyEvent.Base.Timestamp},
                    {"TypeCode", data.FullyEvent.Event.GetType().FullName },
                    {"Data", Encoding.Default.GetString(data.BytesTransport.EventBytes)},
                    {"UniqueId",string.IsNullOrEmpty(data.UniqueId) ? data.FullyEvent.Base.Version.ToString() : data.UniqueId }
                });
            }
            var collectionTask = grainConfig.GetCollection(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            if (!collectionTask.IsCompletedSuccessfully)
                await collectionTask;
            await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, collectionTask.Result.Name).InsertManyAsync(documents);
        }

        public async Task Delete(PrimaryKey stateId, long endVersion, long startTimestamp)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Lte("Version", endVersion);
            var collectionListTask = grainConfig.GetCollectionList();
            if (!collectionListTask.IsCompletedSuccessfully)
                await collectionListTask;
            foreach (var collection in collectionListTask.Result.Where(c => c.CreateTime >= startTimestamp))
            {
                await grainConfig.Storage.GetCollection<BsonDocument>(grainConfig.DataBase, collection.Name).DeleteManyAsync(filter);
            }
        }
    }
}
