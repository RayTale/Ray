using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Ray.Storage.MongoDB
{
    public class StorageConfig
    {
        public string DataBase { get; set; }
        public string EventCollection { get; set; }
        public string SnapshotCollection { get; set; }
        private List<SplitCollectionInfo> AllSplitCollections { get; set; }
        public IMongoStorage Storage { get; }
        const string SplitCollectionName = "SplitCollections";
        readonly bool sharding = false;
        readonly int shardingMilliseconds;
        public StorageConfig(IMongoStorage storage, string database, string eventCollection, string snapshotCollection, bool sharding = false, int shardingDays = 90)
        {
            DataBase = database;
            EventCollection = eventCollection;
            SnapshotCollection = snapshotCollection;
            Storage = storage;
            this.sharding = sharding;
            shardingMilliseconds = shardingDays * 24 * 60 * 60 * 1000;
        }
        public async ValueTask<List<SplitCollectionInfo>> GetCollectionList()
        {
            if (AllSplitCollections == null || AllSplitCollections.Count == 0)
            {
                var collectionTask = GetCollection(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                if (!collectionTask.IsCompleted)
                    await collectionTask;
                return new List<SplitCollectionInfo>() { collectionTask.Result };
            }
            return AllSplitCollections;
        }
        int isBuilded = 0;
        bool buildedResult = false;
        public async ValueTask Build()
        {
            while (!buildedResult)
            {
                if (Interlocked.CompareExchange(ref isBuilded, 1, 0) == 0)
                {
                    try
                    {
                        await CreateIndex();
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
        private async Task CreateIndex()
        {
            var stateCollection = Storage.GetCollection<BsonDocument>(DataBase, SnapshotCollection);
            var stateIndex = await stateCollection.Indexes.ListAsync();
            var stateIndexList = await stateIndex.ToListAsync();
            if (!stateIndexList.Exists(p => p["name"] == "State"))
            {
                await stateCollection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'StateId':1}", new CreateIndexOptions { Name = "State", Unique = true }));
            }
            var collection = Storage.GetCollection<BsonDocument>(DataBase, SplitCollectionName);
            var index = await collection.Indexes.ListAsync();
            var indexList = await index.ToListAsync();
            if (!indexList.Exists(p => p["name"] == "Name"))
            {
                await collection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'Name':1}", new CreateIndexOptions { Name = "Name", Unique = true }));
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

        public async ValueTask<SplitCollectionInfo> GetCollection(long eventTimestamp)
        {
            var lastCollection = AllSplitCollections.LastOrDefault();
            //如果不需要分表，直接返回
            if (lastCollection != null && !sharding) return lastCollection;

            var firstCollection = AllSplitCollections.FirstOrDefault();
            var nowUtcTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var subMilliseconds = eventTimestamp - (firstCollection != null ? firstCollection.CreateTime : nowUtcTime);
            var newVersion = subMilliseconds > 0 ? Convert.ToInt32((subMilliseconds / shardingMilliseconds)) : 0;

            if (lastCollection == null || newVersion > lastCollection.Version)
            {
                var collection = new SplitCollectionInfo
                {
                    Id = ObjectId.GenerateNewId().ToString(),
                    Version = newVersion,
                    Type = EventCollection,
                    CreateTime = nowUtcTime,
                    Name = EventCollection + "_" + newVersion
                };
                try
                {
                    await Storage.GetCollection<SplitCollectionInfo>(DataBase, SplitCollectionName).InsertOneAsync(collection);
                    AllSplitCollections.Add(collection);
                    lastCollection = collection;
                    await CreateEventIndex(collection.Name);
                }
                catch (MongoWriteException ex)
                {
                    if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                    {
                        AllSplitCollections = await (await Storage.GetCollection<SplitCollectionInfo>(DataBase, SplitCollectionName).FindAsync(c => c.Type == EventCollection)).ToListAsync();
                        return await GetCollection(eventTimestamp);
                    }
                }
            }
            return lastCollection;
        }
    }
}
