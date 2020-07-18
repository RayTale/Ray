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
using Ray.Core.Abstractions;
using Ray.Core.Channels;
using Ray.Core.Event;
using Ray.Core.Serialization;
using Ray.Core.Storage;

namespace Ray.Storage.Mongo
{
    public class EventStorage<PrimaryKey> : IEventStorage<PrimaryKey>
    {
        private readonly StorageOptions grainConfig;
        private readonly IMpscChannel<AskInputBox<EventTaskBox<PrimaryKey>, bool>> mpscChannel;
        private readonly ILogger<EventStorage<PrimaryKey>> logger;
        private readonly ISerializer serializer;
        private readonly ITypeFinder typeFinder;

        public EventStorage(IServiceProvider serviceProvider, StorageOptions grainConfig)
        {
            this.serializer = serviceProvider.GetService<ISerializer>();
            this.typeFinder = serviceProvider.GetService<ITypeFinder>();
            this.logger = serviceProvider.GetService<ILogger<EventStorage<PrimaryKey>>>();
            this.mpscChannel = serviceProvider.GetService<IMpscChannel<AskInputBox<EventTaskBox<PrimaryKey>, bool>>>();
            this.mpscChannel.BindConsumer(this.BatchInsertExecuter);
            this.grainConfig = grainConfig;
        }

        public async Task<IList<FullyEvent<PrimaryKey>>> GetList(PrimaryKey stateId, long latestTimestamp, long startVersion, long endVersion)
        {
            var collectionListTask = this.grainConfig.GetCollectionList();
            if (!collectionListTask.IsCompletedSuccessfully)
            {
                await collectionListTask;
            }

            var list = new List<FullyEvent<PrimaryKey>>();
            foreach (var collection in collectionListTask.Result.Where(c => c.EndTime >= latestTimestamp))
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Lte("Version", endVersion) & filterBuilder.Gte("Version", startVersion);
                var cursor = await this.grainConfig.Client.GetCollection<BsonDocument>(this.grainConfig.DataBase, collection.SubTable).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(10000).Token);
                foreach (var document in cursor.ToEnumerable())
                {
                    var typeCode = document["TypeCode"].AsString;
                    var data = document["Data"].AsString;
                    var timestamp = document["Timestamp"].AsInt64;
                    var version = document["Version"].AsInt64;
                    if (version <= endVersion && version >= startVersion)
                    {
                        if (this.serializer.Deserialize(Encoding.UTF8.GetBytes(data), this.typeFinder.FindType(typeCode)) is IEvent evt)
                        {
                            list.Add(new FullyEvent<PrimaryKey>
                            {
                                StateId = stateId,
                                Event = evt,
                                BasicInfo = new EventBasicInfo(version, timestamp)
                            });
                        }
                    }
                }
            }

            return list;
        }

        public async Task<IList<FullyEvent<PrimaryKey>>> GetListByType(PrimaryKey stateId, string typeCode, long startVersion, int limit)
        {
            var collectionListTask = this.grainConfig.GetCollectionList();
            if (!collectionListTask.IsCompletedSuccessfully)
            {
                await collectionListTask;
            }

            var list = new List<FullyEvent<PrimaryKey>>();
            foreach (var collection in collectionListTask.Result)
            {
                var filter = Builders<BsonDocument>.Filter.Eq("StateId", stateId) & Builders<BsonDocument>.Filter.Eq("TypeCode", typeCode) & Builders<BsonDocument>.Filter.Gte("Version", startVersion);
                var cursor = await this.grainConfig.Client.GetCollection<BsonDocument>(this.grainConfig.DataBase, collection.SubTable).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(10000).Token);
                foreach (var document in cursor.ToEnumerable())
                {
                    var data = document["Data"].AsString;
                    var timestamp = document["Timestamp"].AsInt64;
                    var version = document["Version"].AsInt64;
                    if (version >= startVersion && this.serializer.Deserialize(Encoding.UTF8.GetBytes(data), this.typeFinder.FindType(typeCode)) is IEvent evt)
                    {
                        list.Add(new FullyEvent<PrimaryKey>
                        {
                            StateId = stateId,
                            Event = evt,
                            BasicInfo = new EventBasicInfo(version, timestamp)
                        });
                    }
                }

                if (list.Count >= limit)
                {
                    break;
                }
            }

            return list;
        }

        public Task<bool> Append(FullyEvent<PrimaryKey> fullyEvent, string eventJson, string unique)
        {
            var input = new EventTaskBox<PrimaryKey>(fullyEvent, eventJson, unique);
            return Task.Run(async () =>
            {
                var wrap = new AskInputBox<EventTaskBox<PrimaryKey>, bool>(input);
                var writeTask = this.mpscChannel.WriteAsync(wrap);
                if (!writeTask.IsCompletedSuccessfully)
                {
                    await writeTask;
                }

                return await wrap.TaskSource.Task;
            });
        }

        private async Task BatchInsertExecuter(List<AskInputBox<EventTaskBox<PrimaryKey>, bool>> wrapperList)
        {
            var minTimestamp = wrapperList.Min(t => t.Value.Event.BasicInfo.Timestamp);
            var maxTimestamp = wrapperList.Max(t => t.Value.Event.BasicInfo.Timestamp);
            var minTask = this.grainConfig.GetCollection(minTimestamp);
            if (!minTask.IsCompletedSuccessfully)
            {
                await minTask;
            }

            if (minTask.Result.EndTime > maxTimestamp)
            {
                await BatchInsert(minTask.Result.SubTable, wrapperList);
            }
            else
            {
                var groups = (await Task.WhenAll(wrapperList.Select(async t =>
                {
                    var task = this.grainConfig.GetCollection(t.Value.Event.BasicInfo.Timestamp);
                    if (!task.IsCompletedSuccessfully)
                    {
                        await task;
                    }

                    return (task.Result.SubTable, t);
                }))).GroupBy(t => t.SubTable);
                foreach (var group in groups)
                {
                    await BatchInsert(group.Key, group.Select(g => g.t).ToList());
                }
            }

            async Task BatchInsert(string collectionName, List<AskInputBox<EventTaskBox<PrimaryKey>, bool>> list)
            {
                var collection = this.grainConfig.Client.GetCollection<BsonDocument>(this.grainConfig.DataBase, minTask.Result.SubTable);
                var documents = list.Select(wrapper => (wrapper, new BsonDocument
                {
                    { "StateId", BsonValue.Create( wrapper.Value.Event.StateId) },
                    { "Version", wrapper.Value.Event.BasicInfo.Version },
                    { "Timestamp", wrapper.Value.Event.BasicInfo.Timestamp },
                    { "TypeCode", this.typeFinder.GetCode( wrapper.Value.Event.Event.GetType()) },
                    { "Data", wrapper.Value.EventUtf8String },
                    { "UniqueId", string.IsNullOrEmpty(wrapper.Value.UniqueId) ? wrapper.Value.Event.BasicInfo.Version.ToString() : wrapper.Value.UniqueId }
                }));
                var session = await this.grainConfig.Client.Client.StartSessionAsync();
                session.StartTransaction(new MongoDB.Driver.TransactionOptions(readConcern: ReadConcern.Snapshot, writeConcern: WriteConcern.WMajority));
                try
                {
                    await collection.InsertManyAsync(session, documents.Select(d => d.Item2));
                    await session.CommitTransactionAsync();
                    list.ForEach(wrap => wrap.TaskSource.TrySetResult(true));
                }
                catch
                {
                    await session.AbortTransactionAsync();
                    foreach (var document in documents)
                    {
                        try
                        {
                            await collection.InsertOneAsync(document.Item2);
                            document.wrapper.TaskSource.TrySetResult(true);
                        }
                        catch (MongoWriteException ex)
                        {
                            if (ex.WriteError.Category != ServerErrorCategory.DuplicateKey)
                            {
                                document.wrapper.TaskSource.TrySetException(ex);
                            }
                            else
                            {
                                document.wrapper.TaskSource.TrySetResult(false);
                            }
                        }
                    }
                }
            }
        }

        public async Task TransactionBatchAppend(List<EventBox<PrimaryKey>> list)
        {
            var minTimestamp = list.Min(t => t.FullyEvent.BasicInfo.Timestamp);
            var maxTimestamp = list.Max(t => t.FullyEvent.BasicInfo.Timestamp);
            var minTask = this.grainConfig.GetCollection(minTimestamp);
            if (!minTask.IsCompletedSuccessfully)
            {
                await minTask;
            }

            if (minTask.Result.EndTime > maxTimestamp)
            {
                var session = await this.grainConfig.Client.Client.StartSessionAsync();
                session.StartTransaction(new MongoDB.Driver.TransactionOptions(readConcern: ReadConcern.Snapshot, writeConcern: WriteConcern.WMajority));
                try
                {
                    await this.grainConfig.Client.GetCollection<BsonDocument>(this.grainConfig.DataBase, minTask.Result.SubTable).InsertManyAsync(session, list.Select(data => new BsonDocument
                        {
                            { "StateId", BsonValue.Create( data.FullyEvent.StateId) },
                            { "Version", data.FullyEvent.BasicInfo.Version },
                            { "Timestamp", data.FullyEvent.BasicInfo.Timestamp },
                            { "TypeCode", this.typeFinder.GetCode( data.FullyEvent.Event.GetType()) },
                            { "Data", data.EventUtf8String },
                            { "UniqueId", data.UniqueId }
                        }));
                    await session.CommitTransactionAsync();
                }
                catch
                {
                    await session.AbortTransactionAsync();
                    throw;
                }
            }
            else
            {
                var groups = (await Task.WhenAll(list.Select(async t =>
                {
                    var task = this.grainConfig.GetCollection(t.FullyEvent.BasicInfo.Timestamp);
                    if (!task.IsCompletedSuccessfully)
                    {
                        await task;
                    }

                    return (task.Result.SubTable, t);
                }))).GroupBy(t => t.SubTable);
                var session = await this.grainConfig.Client.Client.StartSessionAsync();
                session.StartTransaction(new MongoDB.Driver.TransactionOptions(readConcern: ReadConcern.Snapshot, writeConcern: WriteConcern.WMajority));
                try
                {
                    foreach (var group in groups)
                    {
                        await this.grainConfig.Client.GetCollection<BsonDocument>(this.grainConfig.DataBase, group.Key).InsertManyAsync(session, group.Select(data => new BsonDocument
                            {
                                { "StateId", BsonValue.Create( data.t.FullyEvent.StateId) },
                                { "Version", data.t.FullyEvent.BasicInfo.Version },
                                { "Timestamp", data.t.FullyEvent.BasicInfo.Timestamp },
                                { "TypeCode", this.typeFinder.GetCode( data.t.FullyEvent.Event.GetType()) },
                                { "Data", data.t.EventUtf8String },
                                { "UniqueId", data.t.UniqueId }
                            }));
                    }

                    await session.CommitTransactionAsync();
                }
                catch (Exception ex)
                {
                    await session.AbortTransactionAsync();
                    this.logger.LogError(ex, nameof(this.TransactionBatchAppend));
                    throw;
                }
            }
        }

        public async Task DeletePrevious(PrimaryKey stateId, long toVersion, long startTimestamp)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", stateId) & Builders<BsonDocument>.Filter.Lte("Version", toVersion);
            var collectionListTask = this.grainConfig.GetCollectionList();
            if (!collectionListTask.IsCompletedSuccessfully)
            {
                await collectionListTask;
            }

            var session = await this.grainConfig.Client.Client.StartSessionAsync();
            session.StartTransaction(new MongoDB.Driver.TransactionOptions(readConcern: ReadConcern.Snapshot, writeConcern: WriteConcern.WMajority));
            try
            {
                foreach (var collection in collectionListTask.Result.Where(c => c.EndTime >= startTimestamp))
                {
                    await this.grainConfig.Client.GetCollection<BsonDocument>(this.grainConfig.DataBase, collection.SubTable).DeleteManyAsync(session, filter);
                }

                await session.CommitTransactionAsync();
            }
            catch
            {
                await session.AbortTransactionAsync();
                throw;
            }
        }

        public async Task DeleteAfter(PrimaryKey stateId, long fromVersion, long startTimestamp)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Gte("Version", fromVersion);
            var collectionListTask = this.grainConfig.GetCollectionList();
            if (!collectionListTask.IsCompletedSuccessfully)
            {
                await collectionListTask;
            }

            var session = await this.grainConfig.Client.Client.StartSessionAsync();
            session.StartTransaction(new MongoDB.Driver.TransactionOptions(readConcern: ReadConcern.Snapshot, writeConcern: WriteConcern.WMajority));
            try
            {
                foreach (var collection in collectionListTask.Result.Where(c => c.EndTime >= startTimestamp))
                {
                    await this.grainConfig.Client.GetCollection<BsonDocument>(this.grainConfig.DataBase, collection.SubTable).DeleteManyAsync(session, filter);
                }

                await session.CommitTransactionAsync();
            }
            catch
            {
                await session.AbortTransactionAsync();
                throw;
            }
        }

        public async Task DeleteByVersion(PrimaryKey stateId, long version, long timestamp)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Eq("Version", version);
            var collectionListTask = this.grainConfig.GetCollectionList();
            if (!collectionListTask.IsCompletedSuccessfully)
            {
                await collectionListTask;
            }

            var collection = collectionListTask.Result.SingleOrDefault(t => t.StartTime <= timestamp && t.EndTime >= timestamp);
            await this.grainConfig.Client.GetCollection<BsonDocument>(this.grainConfig.DataBase, collection.SubTable).DeleteOneAsync(filter);
        }
    }
}
