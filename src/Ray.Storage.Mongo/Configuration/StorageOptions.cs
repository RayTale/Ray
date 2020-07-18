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
            this.Client = ClientFactory.CreateClient(serviceProvider.GetService<IOptions<MongoConnections>>().Value.ConnectionDict[connectionKey]);
            this.indexBuildService = serviceProvider.GetService<IIndexBuildService>();
            this.DataBase = database;
            this.UniqueName = uniqueName;
            this.SubTableMillionSecondsInterval = subTableMinutesInterval * 24 * 60 * 60 * 1000;
        }

        public ICustomClient Client { get; }

        public string UniqueName { get; set; }

        public bool Singleton { get; set; }

        public string DataBase { get; set; }

        public string EventCollection => $"{this.UniqueName}_Event";

        public string SnapshotCollection => $"{this.UniqueName}_Snapshot";

        public string SnapshotArchiveTable => $"{this.SnapshotCollection}_Archive";

        public string EventArchiveTable => $"{this.EventCollection}_Archive";

        public long SubTableMillionSecondsInterval { get; set; }

        private bool builded = false;
        private List<SubCollectionInfo> subTables;
        private readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);

        public async ValueTask Build()
        {
            if (!this.builded)
            {
                await this.semaphore.WaitAsync();
                try
                {
                    if (!this.builded)
                    {
                        await this.indexBuildService.CreateSubTableRecordIndex(this.Client, this.DataBase, this.subTableRecordCollection);
                        await this.indexBuildService.CreateSnapshotIndex(this.Client, this.DataBase, this.SnapshotCollection);
                        await this.indexBuildService.CreateSnapshotArchiveIndex(this.Client, this.DataBase, this.SnapshotArchiveTable);
                        await this.indexBuildService.CreateEventArchiveIndex(this.Client, this.DataBase, this.EventArchiveTable);
                        this.subTables = await this.GetSubTables();
                        this.builded = true;
                    }
                }
                finally
                {
                    this.semaphore.Release();
                }
            }
        }

        private async Task<List<SubCollectionInfo>> GetSubTables()
        {
            var filter = Builders<BsonDocument>.Filter.Eq("Table", this.EventCollection);
            var cursor = await this.Client.GetCollection<BsonDocument>(this.DataBase, this.subTableRecordCollection).FindAsync<BsonDocument>(filter, cancellationToken: new CancellationTokenSource(10000).Token);
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
            var lastSubTable = this.subTables.LastOrDefault();
            if (lastSubTable is null || lastSubTable.EndTime <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
            {
                await this.semaphore.WaitAsync();
                try
                {
                    if (lastSubTable is null || lastSubTable.EndTime <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
                    {
                        this.subTables = await this.GetSubTables();
                    }
                }
                finally
                {
                    this.semaphore.Release();
                }
            }

            return this.subTables;
        }

        public async ValueTask<SubCollectionInfo> GetCollection(long eventTimestamp)
        {
            var getTask = this.GetCollectionList();
            if (!getTask.IsCompletedSuccessfully)
            {
                await getTask;
            }

            var subTable = this.SubTableMillionSecondsInterval == 0 ? getTask.Result.LastOrDefault() : getTask.Result.SingleOrDefault(table => table.StartTime <= eventTimestamp && table.EndTime > eventTimestamp);
            if (subTable is null)
            {
                await this.semaphore.WaitAsync();
                subTable = this.SubTableMillionSecondsInterval == 0 ? getTask.Result.LastOrDefault() : getTask.Result.SingleOrDefault(table => table.StartTime <= eventTimestamp && table.EndTime > eventTimestamp);
                try
                {
                    if (subTable is null)
                    {
                        var lastSubTable = getTask.Result.LastOrDefault();
                        var startTime = lastSubTable != null ? (lastSubTable.EndTime == lastSubTable.StartTime ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : lastSubTable.EndTime) : eventTimestamp;
                        var index = lastSubTable is null ? 0 : lastSubTable.Index + 1;
                        subTable = new SubCollectionInfo
                        {
                            Table = this.EventCollection,
                            SubTable = $"{this.EventCollection}_{index}",
                            Index = index,
                            StartTime = startTime,
                            EndTime = startTime + this.SubTableMillionSecondsInterval
                        };
                        try
                        {
                            await this.Client.GetCollection<BsonDocument>(this.DataBase, this.subTableRecordCollection).InsertOneAsync(new BsonDocument
                            {
                                { "Table", subTable.Table },
                                { "SubTable", subTable.SubTable },
                                { "Index", subTable.Index },
                                { "StartTime", subTable.StartTime },
                                { "EndTime", subTable.EndTime }
                            });
                            await this.indexBuildService.CreateEventIndex(this.Client, this.DataBase, subTable.SubTable);
                            this.subTables.Add(subTable);
                        }
                        catch
                        {
                            subTable = default;
                            this.subTables = await this.GetSubTables();
                        }
                    }
                }
                finally
                {
                    this.semaphore.Release();
                }
            }

            if (subTable is null)
            {
                subTable = await this.GetCollection(eventTimestamp);
            }

            return subTable;
        }
    }
}
