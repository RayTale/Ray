using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Ray.Core.Storage;
using Ray.Storage.Mongo.Core;

namespace Ray.Storage.Mongo
{
    public class StorageOptions : IStorageOptions
    {
        private readonly IIndexBuildService indexBuildService;
        private readonly string subTableRecordCollection;
        public StorageOptions(IServiceProvider serviceProvider, string connectionKey, string database, string uniqueName, int subTableMinutesInterval = 30, string subTableRecordCollection = "SubTable")
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
        public string EventArchiveTable => $"{UniqueName}_Archive";
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
            return await (await Client.GetCollection<SubCollectionInfo>(DataBase, subTableRecordCollection).FindAsync(c => c.Table == EventCollection)).ToListAsync();
        }
        public async ValueTask<List<SubCollectionInfo>> GetCollectionList()
        {
            var lastSubTable = _subTables.LastOrDefault();
            if (lastSubTable == default || lastSubTable.EndTime <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
            {
                await semaphore.WaitAsync();
                try
                {
                    if (lastSubTable == default || lastSubTable.EndTime <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
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
            if (subTable == default)
            {
                await semaphore.WaitAsync();
                subTable = SubTableMillionSecondsInterval == 0 ? getTask.Result.LastOrDefault() : getTask.Result.SingleOrDefault(table => table.StartTime <= eventTimestamp && table.EndTime > eventTimestamp);
                try
                {
                    if (subTable == default)
                    {
                        var lastSubTable = getTask.Result.LastOrDefault();
                        var startTime = lastSubTable != default ? (lastSubTable.EndTime == lastSubTable.StartTime ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : lastSubTable.EndTime) : eventTimestamp;
                        var index = lastSubTable == default ? 0 : lastSubTable.Index + 1;
                        subTable = new SubCollectionInfo
                        {
                            Table = EventCollection,
                            SubTable = $"{EventCollection}_index",
                            Index = index,
                            StartTime = startTime,
                            EndTime = startTime + SubTableMillionSecondsInterval
                        };
                        try
                        {
                            await Client.GetCollection<SubCollectionInfo>(DataBase, subTableRecordCollection).InsertOneAsync(subTable);
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
            if (subTable == default)
            {
                subTable = await GetCollection(eventTimestamp);
            }
            return subTable;
        }
    }
}
