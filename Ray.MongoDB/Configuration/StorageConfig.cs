using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Storage;
using Microsoft.Extensions.DependencyInjection;
using Ray.Storage.MongoDB.Core;

namespace Ray.Storage.MongoDB
{
    public class StorageConfig : IStorageConfig
    {
        const string subTableCollectionName = "SubTableCollection";
        public StorageConfig(IServiceProvider serviceProvider, string connectionKey, string database, string uniqueName, int subTableMinutesInterval = 40)
        {
            var connection = serviceProvider.GetService<IOptions<MongoConnections>>().Value.ConnectionDict[connectionKey];
            Storage = ClientFactory.CreateClient(connection);
            DataBase = database;
            UniqueName = uniqueName;
            SubTableMillionSecondsInterval = subTableMinutesInterval * 24 * 60 * 60 * 1000;
        }
        public ICustomClient Storage { get; }
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
                        await CreateSnapshotIndex();
                        await CreateSnapshotArchiveIndex();
                        await CreateEventArchiveIndex();
                        _subTables = await (await Storage.GetCollection<SubCollectionInfo>(DataBase, subTableCollectionName).FindAsync(c => c.Table == EventCollection)).ToListAsync();
                        builded = true;
                    }
                }
                finally
                {
                    semaphore.Release();
                }
            }
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
                        _subTables = await (await Storage.GetCollection<SubCollectionInfo>(DataBase, subTableCollectionName).FindAsync(c => c.Table == EventCollection)).ToListAsync();
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
                            await Storage.GetCollection<SubCollectionInfo>(DataBase, subTableCollectionName).InsertOneAsync(subTable);
                            await CreateEventIndex(subTable.SubTable);
                            _subTables.Add(subTable);
                        }
                        catch
                        {
                            subTable = default;
                            _subTables = await (await Storage.GetCollection<SubCollectionInfo>(DataBase, subTableCollectionName).FindAsync(c => c.Table == EventCollection)).ToListAsync();
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

        private async Task CreateSnapshotIndex()
        {
            var stateCollection = Storage.GetCollection<BsonDocument>(DataBase, SnapshotCollection);
            var stateIndex = await stateCollection.Indexes.ListAsync();
            var stateIndexList = await stateIndex.ToListAsync();
            if (!stateIndexList.Exists(p => p["name"] == "StateId"))
            {
                await stateCollection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'StateId':1}", new CreateIndexOptions { Name = "StateId", Unique = true }));
            }
            var collection = Storage.GetCollection<BsonDocument>(DataBase, subTableCollectionName);
            var index = await collection.Indexes.ListAsync();
            var indexList = await index.ToListAsync();
            if (!indexList.Exists(p => p["name"] == "Name"))
            {
                await collection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'Name':1}", new CreateIndexOptions { Name = "Name", Unique = true }));
            }
        }
        private async Task CreateSnapshotArchiveIndex()
        {
            var stateCollection = Storage.GetCollection<BsonDocument>(DataBase, SnapshotArchiveTable);
            var stateIndex = await stateCollection.Indexes.ListAsync();
            var stateIndexList = await stateIndex.ToListAsync();
            if (!stateIndexList.Exists(p => p["name"] == "StateId"))
            {
                await stateCollection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'StateId':1}", new CreateIndexOptions { Name = "StateId", Unique = false }));
            }
        }
        private async Task CreateEventIndex(string collectionName)
        {
            var collectionService = Storage.GetCollection<BsonDocument>(DataBase, collectionName);
            var indexList = await (await collectionService.Indexes.ListAsync()).ToListAsync();
            if (!indexList.Exists(p => p["name"] == "State_Version") && !indexList.Exists(p => p["name"] == "State_UniqueId"))
            {
                await collectionService.Indexes.CreateManyAsync(
                      new List<CreateIndexModel<BsonDocument>>() {
                new CreateIndexModel<BsonDocument>("{'StateId':1,'Version':1}", new CreateIndexOptions { Name = "State_Version",Unique=true }),
                new CreateIndexModel<BsonDocument>("{'StateId':1,'TypeCode':1,'UniqueId':1}", new CreateIndexOptions { Name = "State_UniqueId", Unique = true }) }
                      );
            }
        }
        private async Task CreateEventArchiveIndex()
        {
            var collectionService = Storage.GetCollection<BsonDocument>(DataBase, EventArchiveTable);
            var indexList = await (await collectionService.Indexes.ListAsync()).ToListAsync();
            if (!indexList.Exists(p => p["name"] == "State_Version") && !indexList.Exists(p => p["name"] == "State_UniqueId"))
            {
                await collectionService.Indexes.CreateManyAsync(
                      new List<CreateIndexModel<BsonDocument>>() {
                new CreateIndexModel<BsonDocument>("{'StateId':1,'Version':1}", new CreateIndexOptions { Name = "State_Version",Unique=true }),
                new CreateIndexModel<BsonDocument>("{'StateId':1,'TypeCode':1,'UniqueId':1}", new CreateIndexOptions { Name = "State_UniqueId", Unique = true }) }
                      );
            }
        }
    }
}
