using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Storage;

namespace Ray.Storage.MongoDB
{
    public class StorageConfig : IStorageConfig
    {
        public string DataBase { get; set; }
        public string EventCollection { get; set; }
        public string SnapshotCollection { get; set; }
        public string FollowName { get; set; }
        public bool IsFollow { get; set; }
        public string ArchiveSnapshotTable => $"{SnapshotCollection}_Archive";
        private List<SplitCollectionInfo> AllSplitCollections { get; set; }
        public IMongoStorage Storage { get; }
        public bool Singleton { get; set; }

        const string SplitCollectionName = "SplitCollections";
        readonly bool sharding = false;
        readonly int shardingMinutes;
        public StorageConfig(IMongoStorage storage, string database, string eventCollection, string snapshotCollection, bool isFollow = false, string followName = null, bool sharding = false, int shardingDays = 90)
        {
            DataBase = database;
            EventCollection = eventCollection;
            SnapshotCollection = snapshotCollection;
            Storage = storage;
            FollowName = followName;
            this.sharding = sharding;
            IsFollow = isFollow;
            shardingMinutes = shardingDays * 24 * 60;
        }
        public async ValueTask<List<SplitCollectionInfo>> GetCollectionList()
        {
            if (AllSplitCollections == null || AllSplitCollections.Count == 0)
            {
                var collectionTask = GetCollection(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                if (!collectionTask.IsCompletedSuccessfully)
                    await collectionTask;
                return new List<SplitCollectionInfo>() { collectionTask.Result };
            }
            return AllSplitCollections;
        }
        public string GetFollowStateTable()
        {
            return $"{SnapshotCollection}_{FollowName}";
        }
        int isBuilded = 0;
        bool buildedResult = false;
        public async ValueTask Init()
        {
            while (!buildedResult)
            {
                if (Interlocked.CompareExchange(ref isBuilded, 1, 0) == 0)
                {
                    try
                    {
                        if (!IsFollow)
                        {
                            await CreateSnapshotIndex();
                            await CreateArchiveSnapshotIndex();
                        }
                        else if (!string.IsNullOrEmpty(FollowName))
                        {
                            await CreateFollowSnapshotIndex();
                        }
                        await CreateSnapshotIndex();
                        AllSplitCollections = await (await Storage.GetCollection<SplitCollectionInfo>(DataBase, SplitCollectionName).FindAsync(c => c.Type == EventCollection)).ToListAsync();
                        buildedResult = true;
                    }
                    finally
                    {
                        Interlocked.Exchange(ref isBuilded, 0);
                    }
                }
                await Task.Delay(50);
            }
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
            var collection = Storage.GetCollection<BsonDocument>(DataBase, SplitCollectionName);
            var index = await collection.Indexes.ListAsync();
            var indexList = await index.ToListAsync();
            if (!indexList.Exists(p => p["name"] == "Name"))
            {
                await collection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'Name':1}", new CreateIndexOptions { Name = "Name", Unique = true }));
            }
        }
        private async Task CreateFollowSnapshotIndex()
        {
            var stateCollection = Storage.GetCollection<BsonDocument>(DataBase, GetFollowStateTable());
            var stateIndex = await stateCollection.Indexes.ListAsync();
            var stateIndexList = await stateIndex.ToListAsync();
            if (!stateIndexList.Exists(p => p["name"] == "State"))
            {
                await stateCollection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'StateId':1}", new CreateIndexOptions { Name = "State", Unique = false }));
            }
        }
        private async Task CreateArchiveSnapshotIndex()
        {
            var stateCollection = Storage.GetCollection<BsonDocument>(DataBase, ArchiveSnapshotTable);
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
        public int GetVersion(long eventTimestamp)
        {
            var firstTable = AllSplitCollections.FirstOrDefault();
            //如果不需要分表，直接返回
            if (firstTable != null && !sharding) return 0;
            var nowUtcTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var subMinutes = (eventTimestamp - (firstTable != null ? firstTable.CreateTime : nowUtcTime)) / (60 * 1000);
            return subMinutes > 0 ? (int)(subMinutes / shardingMinutes) : 0;
        }
        public async ValueTask<SplitCollectionInfo> GetCollection(long eventTimestamp)
        {
            var firstCollection = AllSplitCollections.FirstOrDefault();
            //如果不需要分表，直接返回
            if (firstCollection != null && !sharding) return firstCollection;

            var nowUtcTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var subMinutes = (eventTimestamp - (firstCollection != null ? firstCollection.CreateTime : nowUtcTime)) / (60 * 1000);
            var version = subMinutes > 0 ? Convert.ToInt32((subMinutes / shardingMinutes)) : 0;
            var resultTable = AllSplitCollections.FirstOrDefault(t => t.Version == version);
            if (resultTable == default)
            {
                var collection = new SplitCollectionInfo
                {
                    Id = ObjectId.GenerateNewId().ToString(),
                    Version = version,
                    Type = EventCollection,
                    CreateTime = nowUtcTime,
                    Name = EventCollection + "_" + version
                };
                try
                {
                    await Storage.GetCollection<SplitCollectionInfo>(DataBase, SplitCollectionName).InsertOneAsync(collection);
                    AllSplitCollections.Add(collection);
                    resultTable = collection;
                    await CreateEventIndex(collection.Name);
                }
                catch (MongoWriteException ex)
                {
                    if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                    {
                        AllSplitCollections = await (await Storage.GetCollection<SplitCollectionInfo>(DataBase, SplitCollectionName).FindAsync(c => c.Type == EventCollection)).ToListAsync();
                        return await GetCollection(eventTimestamp);
                    }
                    resultTable = AllSplitCollections.First(t => t.Version == version);
                }
            }
            return resultTable;
        }
    }
}
