using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Ray.Core.Serialization;
using Ray.Core.Snapshot;
using Ray.Core.Storage;

namespace Ray.Storage.Mongo
{
    public class ArchiveStorage<PrimaryKey, StateType> : IArchiveStorage<PrimaryKey, StateType>
           where StateType : class, new()
    {
        readonly StorageOptions grainConfig;
        readonly ISerializer serializer;
        public ArchiveStorage(ISerializer serializer, StorageOptions grainConfig)
        {
            this.serializer = serializer;
            this.grainConfig = grainConfig;
        }
        public Task Delete(PrimaryKey stateId, string briefId)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", stateId) & Builders<BsonDocument>.Filter.Eq("Id", briefId);
            return grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotArchiveTable).DeleteOneAsync(filter);
        }

        public Task DeleteAll(PrimaryKey stateId)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", stateId);
            return grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotArchiveTable).DeleteManyAsync(filter);
        }

        public Task EventIsClear(PrimaryKey stateId, string briefId)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", stateId) & Builders<BsonDocument>.Filter.Eq("Id", briefId);
            var update = Builders<BsonDocument>.Update.Set("EventIsCleared", true);
            return grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotArchiveTable).UpdateOneAsync(filter, update);
        }

        public async Task<List<ArchiveBrief>> GetBriefList(PrimaryKey stateId)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", stateId);
            var cursor = await grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotArchiveTable).FindAsync<BsonDocument>(filter);
            var result = new List<ArchiveBrief>();
            await cursor.ForEachAsync(doc =>
            {
                result.Add(new ArchiveBrief
                {
                    Id = doc["Id"].AsString,
                    StartTimestamp = doc["StartTimestamp"].AsInt64,
                    EndTimestamp = doc["EndTimestamp"].AsInt64,
                    StartVersion = doc["StartVersion"].AsInt64,
                    EndVersion = doc["EndVersion"].AsInt64,
                    EventIsCleared = doc["EventIsCleared"].AsBoolean,
                    Index = doc["Index"].AsInt32
                });
            });
            return result;
        }

        public async Task<ArchiveBrief> GetLatestBrief(PrimaryKey stateId)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", stateId);
            var sort = Builders<BsonDocument>.Sort.Descending("Index");
            var doc = await grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotArchiveTable).Find(filter).Sort(sort).FirstOrDefaultAsync();
            if (doc != default)
            {
                return new ArchiveBrief
                {
                    Id = doc["Id"].AsString,
                    StartTimestamp = doc["StartTimestamp"].AsInt64,
                    EndTimestamp = doc["EndTimestamp"].AsInt64,
                    StartVersion = doc["StartVersion"].AsInt64,
                    EndVersion = doc["EndVersion"].AsInt64,
                    EventIsCleared = doc["EventIsCleared"].AsBoolean,
                    Index = doc["Index"].AsInt32
                };
            }
            return default;
        }

        public async Task<Snapshot<PrimaryKey, StateType>> GetById(string briefId)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("id", briefId);
            var doc = await grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotArchiveTable).Find(filter).SingleOrDefaultAsync();
            if (doc != default)
            {
                return new Snapshot<PrimaryKey, StateType>()
                {
                    Base = new SnapshotBase<PrimaryKey>
                    {
                        StateId = serializer.Deserialize<PrimaryKey>(doc["StateId"].AsString),
                        Version = doc["Version"].AsInt64,
                        DoingVersion = doc["DoingVersion"].AsInt64,
                        IsLatest = false,
                        IsOver = doc["IsOver"].AsBoolean,
                        StartTimestamp = doc["StartTimestamp"].AsInt64,
                        LatestMinEventTimestamp = doc["LatestMinEventTimestamp"].AsInt64
                    },
                    State = serializer.Deserialize<StateType>(doc["Data"].AsString)
                };
            }
            return default;
        }

        public Task Insert(ArchiveBrief brief, Snapshot<PrimaryKey, StateType> snapshot)
        {
            var doc = new BsonDocument
            {
                { "Id", brief.Id },
                { "StateId", BsonValue.Create(snapshot.Base.StateId) },
                { "StartVersion", brief.StartVersion },
                { "EndVersion", brief.EndVersion },
                { "StartTimestamp", brief.StartTimestamp },
                { "EndTimestamp", brief.EndTimestamp },
                { "Index", brief.Index },
                { "EventIsCleared", brief.EventIsCleared },
                { "Data", serializer.SerializeToString(snapshot.State) },
                { "IsOver", snapshot.Base.IsOver },
                { "Version", snapshot.Base.Version }
            };
            return grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotArchiveTable).InsertOneAsync(doc);
        }

        public Task Over(PrimaryKey stateId, bool isOver)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("StateId", stateId);
            var update = Builders<BsonDocument>.Update.Set("IsOver", isOver);
            return grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.SnapshotArchiveTable).UpdateOneAsync(filter, update);
        }
        public async Task EventArichive(PrimaryKey stateId, long endVersion, long startTimestamp)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("StateId", stateId) & filterBuilder.Lte("Version", endVersion);
            var collectionListTask = grainConfig.GetCollectionList();
            if (!collectionListTask.IsCompletedSuccessfully)
                await collectionListTask;
            var session = await grainConfig.Client.Client.StartSessionAsync();
            session.StartTransaction(new global::MongoDB.Driver.TransactionOptions(readConcern: ReadConcern.Snapshot, writeConcern: WriteConcern.WMajority));
            try
            {
                var archiveCollection = grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, grainConfig.EventArchiveTable);
                foreach (var collection in collectionListTask.Result.Where(c => c.EndTime >= startTimestamp))
                {
                    var deleteCollection = grainConfig.Client.GetCollection<BsonDocument>(grainConfig.DataBase, collection.SubTable);
                    await archiveCollection.InsertManyAsync(session, await (await deleteCollection.FindAsync<BsonDocument>(filter)).ToListAsync());
                    await deleteCollection.DeleteManyAsync(session, filter);
                }
                await session.CommitTransactionAsync();
            }
            catch
            {
                await session.AbortTransactionAsync();
                throw;
            }
        }
    }
}
