using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using MongoDB.Bson;

namespace Ray.MongoES
{
    [AttributeUsage(AttributeTargets.Class)]
    public class MongoStorageAttribute : Attribute
    {
        public string EventDataBase { get; set; }
        public string EventCollection { get; set; }
        public string SnapshotCollection { get; set; }

        const string C_CName = "CollectionInfo";
        bool sharding = false;
        int shardingDays;
        public MongoStorageAttribute(
            string eventDatabase, string collection, bool sharding = false, int shardingDays = 90)
        {
            this.EventDataBase = eventDatabase;
            this.EventCollection = collection + "Event";
            this.SnapshotCollection = collection + "State";
            this.sharding = sharding;
            this.shardingDays = shardingDays;
            CreateCollectionIndex();//创建分表索引
            CreateStateIndex();//创建快照索引
        }
        public List<CollectionInfo> GetCollectionList(DateTime? startTime = null)
        {
            List<CollectionInfo> list = null;
            if (startTime == null)
                list = GetAllCollectionList();
            else
            {
                var collection = GetCollection(startTime.Value);
                list = GetAllCollectionList().Where(c => c.Version >= collection.Version).ToList();
            }
            if (list == null)
            {
                list = new List<CollectionInfo>() { GetCollection(DateTime.UtcNow) };
            }
            return list;
        }
        private void CreateStateIndex()
        {
            Task.Run(async () =>
            {
                var collectionService = MongoStorage.GetCollection<BsonDocument>(EventDataBase, SnapshotCollection);
                CancellationTokenSource cancel = new CancellationTokenSource(1);
                var index = await collectionService.Indexes.ListAsync();
                var indexList = await index.ToListAsync();
                if (!indexList.Exists(p => p["name"] == "State"))
                {
                    await collectionService.Indexes.CreateOneAsync("{'StateId':1}", new CreateIndexOptions { Name = "State", Unique = true });
                }
            }).ConfigureAwait(false);
        }
        private List<CollectionInfo> collectionList;
        public List<CollectionInfo> GetAllCollectionList()
        {
            if (collectionList == null)
            {
                lock (collectionLock)
                {
                    if (collectionList == null)
                    {
                        collectionList = MongoStorage.GetCollection<CollectionInfo>(EventDataBase, C_CName).Find<CollectionInfo>(c => c.Type == EventCollection).ToList();
                    }
                }
            }
            return collectionList;
        }
        private void CreateCollectionIndex()
        {
            Task.Run(async () =>
            {
                var collectionService = MongoStorage.GetCollection<BsonDocument>(EventDataBase, C_CName);
                CancellationTokenSource cancel = new CancellationTokenSource(1);
                var index = await collectionService.Indexes.ListAsync();
                var indexList = await index.ToListAsync();
                if (!indexList.Exists(p => p["name"] == "Name"))
                {
                    await collectionService.Indexes.CreateOneAsync("{'Name':1}", new CreateIndexOptions { Name = "Name", Unique = true });
                }
            }).ConfigureAwait(false);
        }
        private void CreateEventIndex(string collectionName)
        {
            var collectionService = MongoStorage.GetCollection<BsonDocument>(EventDataBase, collectionName);
            var indexList = collectionService.Indexes.List().ToList();
            if (!indexList.Exists(p => p["name"] == "State_Version") && !indexList.Exists(p => p["name"] == "State_MsgId"))
            {
                collectionService.Indexes.CreateMany(
                    new List<CreateIndexModel<BsonDocument>>() {
                new CreateIndexModel<BsonDocument>("{'StateId':1,'Version':1}", new CreateIndexOptions { Name = "State_Version",Unique=true }),
                new CreateIndexModel<BsonDocument>("{'StateId':1,'TypeCode':1,'MsgId':1}", new CreateIndexOptions { Name = "State_MsgId", Unique = true }) }
                    );
            }
        }
        object collectionLock = new object();
        static DateTime startTime = new DateTime(2017, 8, 30);
        public CollectionInfo GetCollection(DateTime eventTime)
        {
            CollectionInfo lastCollection = null;
            var cList = GetAllCollectionList();
            if (cList.Count > 0) lastCollection = cList.Last();
            //如果不需要分表，直接返回
            if (lastCollection != null && !this.sharding) return lastCollection;
            var subTime = eventTime.Subtract(startTime);
            var cVersion = subTime.TotalDays > 0 ? Convert.ToInt32(Math.Floor(subTime.TotalDays / shardingDays)) : 0;
            if (lastCollection == null || cVersion > lastCollection.Version)
            {
                lock (collectionLock)
                {
                    if (lastCollection == null || cVersion > lastCollection.Version)
                    {
                        var collection = new CollectionInfo();
                        collection.Id = ObjectId.GenerateNewId().ToString();
                        collection.Version = cVersion;
                        collection.Type = EventCollection;
                        collection.CreateTime = DateTime.UtcNow;
                        collection.Name = EventCollection + "_" + cVersion;
                        try
                        {
                            MongoStorage.GetCollection<CollectionInfo>(EventDataBase, C_CName).InsertOne(collection);
                            collectionList.Add(collection);
                            lastCollection = collection;
                            CreateEventIndex(collection.Name);
                        }
                        catch (MongoWriteException ex)
                        {
                            if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                            {
                                collectionList = null;
                                return GetCollection(eventTime);
                            }
                        }
                    }
                }
            }
            return lastCollection;
        }
    }
}
