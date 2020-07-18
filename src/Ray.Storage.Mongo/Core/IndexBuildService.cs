using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Ray.Storage.Mongo.Core
{
    public class IndexBuildService : IIndexBuildService
    {
        public async Task CreateSubTableRecordIndex(ICustomClient client, string database, string collectionName)
        {
            var collection = client.GetCollection<BsonDocument>(database, collectionName);
            var indexList = await (await collection.Indexes.ListAsync()).ToListAsync();
            if (!indexList.Exists(p => p["name"] == "Name"))
            {
                await collection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'Name':1}", new CreateIndexOptions { Name = "Name", Unique = true }));
            }
        }
        public async Task CreateSnapshotIndex(ICustomClient client, string database, string collectionName)
        {
            var collection = client.GetCollection<BsonDocument>(database, collectionName);
            var indexList = await (await collection.Indexes.ListAsync()).ToListAsync();
            if (!indexList.Exists(p => p["name"] == "StateId"))
            {
                await collection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'StateId':1}", new CreateIndexOptions { Name = "StateId", Unique = true }));
            }
        }
        public async Task CreateSnapshotArchiveIndex(ICustomClient client, string database, string collectionName)
        {
            var collection = client.GetCollection<BsonDocument>(database, collectionName);
            var indexList = await (await collection.Indexes.ListAsync()).ToListAsync();
            if (!indexList.Exists(p => p["name"] == "StateId"))
            {
                await collection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'StateId':1}", new CreateIndexOptions { Name = "StateId", Unique = false }));
            }
        }
        public async Task CreateEventIndex(ICustomClient client, string database, string collectionName)
        {
            var collection = client.GetCollection<BsonDocument>(database, collectionName);
            var indexList = await (await collection.Indexes.ListAsync()).ToListAsync();
            if (!indexList.Exists(p => p["name"] == "State_Version") && !indexList.Exists(p => p["name"] == "State_UniqueId"))
            {
                await collection.Indexes.CreateManyAsync(
                      new List<CreateIndexModel<BsonDocument>>() {
                new CreateIndexModel<BsonDocument>("{'StateId':1,'Version':1}", new CreateIndexOptions { Name = "State_Version",Unique=true }),
                new CreateIndexModel<BsonDocument>("{'StateId':1,'TypeCode':1,'UniqueId':1}", new CreateIndexOptions { Name = "State_UniqueId", Unique = true }) }
                      );
            }
        }
        public async Task CreateEventArchiveIndex(ICustomClient client, string database, string collectionName)
        {
            var collection = client.GetCollection<BsonDocument>(database, collectionName);
            var indexList = await (await collection.Indexes.ListAsync()).ToListAsync();
            if (!indexList.Exists(p => p["name"] == "State_Version") && !indexList.Exists(p => p["name"] == "State_UniqueId"))
            {
                await collection.Indexes.CreateManyAsync(
                      new List<CreateIndexModel<BsonDocument>>() {
                new CreateIndexModel<BsonDocument>("{'StateId':1,'Version':1}", new CreateIndexOptions { Name = "State_Version",Unique=true }),
                new CreateIndexModel<BsonDocument>("{'StateId':1,'TypeCode':1,'UniqueId':1}", new CreateIndexOptions { Name = "State_UniqueId", Unique = true }) }
                      );
            }
        }
        public async Task CreateTransactionStorageIndex(ICustomClient client, string database, string collectionName)
        {
            var collection = client.GetCollection<BsonDocument>(database, collectionName);
            var indexList = await (await collection.Indexes.ListAsync()).ToListAsync();
            if (!indexList.Exists(p => p["name"] == "UnitName_TransId"))
            {
                await collection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>("{'UnitName':1,'TransactionId':1}", new CreateIndexOptions { Name = "UnitName_TransId", Unique = true }));
            }
        }
    }
}
