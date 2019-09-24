using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Storage;
using Ray.Storage.Mongo.Core;

namespace Ray.Storage.Mongo
{
    public class StorageOptions : IStorageOptions
    {
        private readonly IIndexBuildService indexBuildService;
        private readonly string subTableRecordCollection;
        public StorageOptions(IServiceProvider serviceProvider, string connectionKey, string database, string uniqueName, long subTableMinutesInterval = 30, string subTableRecordCollection = "SubTable")
        {
            this.subTableRecordCollection = subTableRecordCollection;
            Client = ClientFactory.CreateClient(serviceProvider.GetService<IOptions<MongoConnections>>().Value.ConnectionDict[connectionKey]);
            indexBuildService = serviceProvider.GetService<IIndexBuildService>();
            DataBase = database;
            UniqueName = uniqueName;
            SubTableMillionSecondsInterval = subTableMinutesInterval * 24 * 60 * 60 * 1000;
        }
        public ICustomClient Client { get; }
        public string UniqueName { get; set; }
        public bool Singleton { get; set; }
        public string DataBase { get; set; }
        public string EventCollection => $"{UniqueName}_Event";
        public string SnapshotCollection => $"{UniqueName}_Snapshot";
        public string SnapshotArchiveTable => $"{SnapshotCollection}_Archive";
        public string EventArchiveTable => $"{EventCollection}_Archive";
        public long SubTableMillionSecondsInterval { get; set; }

        bool builded = false;
        private List<SubCollectionInfo> _subTables;
        readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);
        public async ValueTask Build()
        {
            if (!builded)
            {
                await semaphore.WaitAsync();
                try
                {
                    if (!builded)
                    {
                        await indexBuildService.CreateSubTableRecordIndex(Client, DataBase, subTableRecordCollection);
                        await indexBuildService.CreateSnapshotIndex(Client, DataBase, SnapshotCollection);
                        await indexBuildService.CreateSnapshotArchiveIndex(Client, DataBase, SnapshotArchiveTable);
                        await indexBuildService.CreateEventArchiveIndex(Client, DataBase, EventArchiveTable);
                        _subTables = await GetSubTables();
                        builded = true;
                    }
                }
                finally
                {
                    semaphore.Release();
                }
            }
        }
        private async Task<List<SubCollectionInfo>> GetSubTables()
        {
            var filter = Builders<BsonDocument>.Filter.Eq("Table", EventCollection);
            var cursor = await Client.GetCollection<BsonDocument>(DataBase, subTableRecordCollection).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(10000).Token);
            var list = new List<SubCollectionInfo>();
            foreach (var document in cursor.ToEnumerable())
            {
                list.Add(new SubCollectionInfo
                {
                    Table = document["Table"].AsString,
                    SubTable = document["SubTable"].AsString,
                    Index = document["Index"].AsInt32,
                    StartTime = document["StartTime"].AsInt64,
                    EndTime = document["EndTime"].AsInt64
                });
            }
            return list;
        }
        public async ValueTask<List<SubCollectionInfo>> GetCollectionList()
        {
            var lastSubTable = _subTables.LastOrDefault();
            if (lastSubTable is null || lastSubTable.EndTime <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
            {
                await semaphore.WaitAsync();
                try
                {
                    if (lastSubTable is null || lastSubTable.EndTime <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
                    {
                        _subTables = await GetSubTables();
                    }
                }
                finally
                {
                    semaphore.Release();
                }
            }
            return _subTables;
        }
        public async ValueTask<SubCollectionInfo> GetCollection(long eventTimestamp)
        {
            var getTask = GetCollectionList();
            if (!getTask.IsCompletedSuccessfully)
                await getTask;
            var subTable = SubTableMillionSecondsInterval == 0 ? getTask.Result.LastOrDefault() : getTask.Result.SingleOrDefault(table => table.StartTime <= eventTimestamp && table.EndTime > eventTimestamp);
            if (subTable is null)
            {
                await semaphore.WaitAsync();
                subTable = SubTableMillionSecondsInterval == 0 ? getTask.Result.LastOrDefault() : getTask.Result.SingleOrDefault(table => table.StartTime <= eventTimestamp && table.EndTime > eventTimestamp);
                try
                {
                    if (subTable is null)
                    {
                        var lastSubTable = getTask.Result.LastOrDefault();
                        var startTime = lastSubTable != null? (lastSubTable.EndTime == lastSubTable.StartTime ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : lastSubTable.EndTime) : eventTimestamp;
                        var index = lastSubTable is null ? 0 : lastSubTable.Index + 1;
                        subTable = new SubCollectionInfo
                        {
                            Table = EventCollection,
                            SubTable = $"{EventCollection}_{index}",
                            Index = index,
                            StartTime = startTime,
                            EndTime = startTime + SubTableMillionSecondsInterval
                        };
                        try
                        {
                            await Client.GetCollection<BsonDocument>(DataBase, subTableRecordCollection).InsertOneAsync(new BsonDocument
                            {
                                { "Table",subTable.Table},
                                { "SubTable",subTable.SubTable},
                                { "Index",subTable.Index},
                                { "StartTime",subTable.StartTime},
                                { "EndTime",subTable.EndTime}
                            });
                            await indexBuildService.CreateEventIndex(Client, DataBase, subTable.SubTable);
                            _subTables.Add(subTable);
                        }
                        catch
                        {
                            subTable = default;
                            _subTables = await GetSubTables();
                        }
                    }
                }
                finally
                {
                    semaphore.Release();
                }
            }
            if (subTable is null)
            {
                subTable = await GetCollection(eventTimestamp);
            }
            return subTable;
        }
    }
}
