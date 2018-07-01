using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.EventSourcing;
using System;
using System.Threading.Tasks;

namespace Ray.MongoDB
{
    public class MongoStorageContainer : IStorageContainer
    {
        ILoggerFactory loggerFactory; IMongoStorage mongoStorage;
        public MongoStorageContainer(ILoggerFactory loggerFactory, IMongoStorage mongoStorage)
        {
            this.loggerFactory = loggerFactory;
            this.mongoStorage = mongoStorage;
        }
        private async Task<MongoGrainConfig> GetESMongoInfo<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            if (grain is IMongoGrain mongoGrain && mongoGrain.GrainConfig != null)
            {
                await mongoGrain.GrainConfig.CreateIndex(mongoStorage);
                return mongoGrain.GrainConfig;
            }
            return null;
        }
        public async ValueTask<IEventStorage<K>> GetEventStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var mongoInfo = await GetESMongoInfo<K, S>(type, grain);
            if (mongoInfo != null)
            {
                return new MongoEventStorage<K>(mongoStorage, loggerFactory.CreateLogger<MongoEventStorage<K>>(), mongoInfo);
            }
            else
                throw new Exception("Not find MongoGrainConfig");
        }
        public async ValueTask<IStateStorage<S, K>> GetStateStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var mongoInfo = await GetESMongoInfo<K, S>(type, grain);
            if (mongoInfo != null)
            {
                return new MongoStateStorage<S, K>(mongoStorage, mongoInfo.EventDataBase, mongoInfo.SnapshotCollection);
            }
            else
                throw new Exception("Not find MongoGrainConfig");
        }
    }
}
