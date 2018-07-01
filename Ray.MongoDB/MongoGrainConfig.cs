using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;
using MongoDB.Bson;

namespace Ray.MongoDB
{
    public class MongoGrainConfig
    {
        public string EventDataBase { get; set; }
        public string EventCollection { get; set; }
        public string SnapshotCollection { get; set; }
        public bool IndexCreated { get; set; }
        const string C_CName = "CollectionInfo";
        readonly bool sharding = false;
        readonly int shardingDays;
        public MongoGrainConfig(
            string eventDatabase, string eventCollection, string snapshotCollection, bool sharding = false, int shardingDays = 90)
        {
            EventDataBase = eventDatabase;
            EventCollection = eventCollection;
            SnapshotCollection = snapshotCollection;
            this.sharding = sharding;
            this.shardingDays = shardingDays;
        }
        public List<CollectionInfo> GetCollectionList(IMongoStorage storage, DateTime sysStartTime, DateTime? startTime = null)
        {
            List<CollectionInfo> list = null;
            if (startTime == null)
                list = GetAllCollectionList(storage);
            else
            {
                var collection = GetCollection(storage, sysStartTime, startTime.Value);
                list = GetAllCollectionList(storage).Where(c => c.Version >= collection.Version).ToList();
            }
            if (list == null || list.Count == 0)
            {
                list = new List<CollectionInfo>() { GetCollection(storage, sysStartTime, DateTime.UtcNow) };
            }
            return list;
        }
        public async Task CreateIndex(IMongoStorage storage)
        {
            if (!IndexCreated)
            {
                var stateCollection = storage.GetCollection<BsonDocument>(EventDataBase, SnapshotCollection);
                var stateIndex = await stateCollection.Indexes.ListAsync();
                var stateIndexList = await stateIndex.ToListAsync();
                if (!stateIndexList.Exists(p => p["name"] == "State"))
                {
                    await stateCollection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'StateId':1}", new CreateIndexOptions { Name = "State", Unique = true }));
                }
                var collection = storage.GetCollection<BsonDocument>(EventDataBase, C_CName);
                var index = await collection.Indexes.ListAsync();
                var indexList = await index.ToListAsync();
                if (!indexList.Exists(p => p["name"] == "Name"))
                {
                    await collection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'Name':1}", new CreateIndexOptions { Name = "Name", Unique = true }));
                }
                IndexCreated = true;
            }
        }
        public async Task CreateEventIndex(IMongoStorage storage, string collectionName)
        {
            var collectionService = storage.GetCollection<BsonDocument>(EventDataBase, collectionName);
            var indexList = (await collectionService.Indexes.ListAsync()).ToList();
            if (!indexList.Exists(p => p["name"] == "State_Version") && !indexList.Exists(p => p["name"] == "State_UniqueId"))
            {
                await collectionService.Indexes.CreateManyAsync(
                      new List<CreateIndexModel<BsonDocument>>() {
                new CreateIndexModel<BsonDocument>("{'StateId':1,'Version':1}", new CreateIndexOptions { Name = "State_Version",Unique=true }),
                new CreateIndexModel<BsonDocument>("{'StateId':1,'TypeCode':1,'UniqueId':1}", new CreateIndexOptions { Name = "State_UniqueId", Unique = true }) }
                      );
            }
        }
        private List<CollectionInfo> collectionList;
        public List<CollectionInfo> GetAllCollectionList(IMongoStorage storage)
        {
            if (collectionList == null)
            {
                lock (collectionLock)
                {
                    if (collectionList == null)
                    {
                        collectionList = storage.GetCollection<CollectionInfo>(EventDataBase, C_CName).Find<CollectionInfo>(c => c.Type == EventCollection).ToList();
                    }
                }
            }
            return collectionList;
        }
        object collectionLock = new object();
        public CollectionInfo GetCollection(IMongoStorage storage, DateTime sysStartTime, DateTime eventTime)
        {
            CollectionInfo lastCollection = null;
            var cList = GetAllCollectionList(storage);
            if (cList.Count > 0) lastCollection = cList.Last();
            //如果不需要分表，直接返回
            if (lastCollection != null && !this.sharding) return lastCollection;
            var subTime = eventTime.Subtract(sysStartTime);
            var cVersion = subTime.TotalDays > 0 ? Convert.ToInt32(Math.Floor(subTime.TotalDays / shardingDays)) : 0;
            if (lastCollection == null || cVersion > lastCollection.Version)
            {
                lock (collectionLock)
                {
                    if (lastCollection == null || cVersion > lastCollection.Version)
                    {
                        var collection = new CollectionInfo
                        {
                            Id = ObjectId.GenerateNewId().ToString(),
                            Version = cVersion,
                            Type = EventCollection,
                            CreateTime = DateTime.UtcNow,
                            Name = EventCollection + "_" + cVersion
                        };
                        try
                        {
                            storage.GetCollection<CollectionInfo>(EventDataBase, C_CName).InsertOne(collection);
                            collectionList.Add(collection);
                            lastCollection = collection;
                            CreateEventIndex(storage, collection.Name).GetAwaiter().GetResult();
                        }
                        catch (MongoWriteException ex)
                        {
                            if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                            {
                                collectionList = null;
                                return GetCollection(storage, sysStartTime, eventTime);
                            }
                        }
                    }
                }
            }
            return lastCollection;
        }
    }
}
