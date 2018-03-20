using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.EventSourcing;
using System;

namespace Ray.MongoDb
{
    public class MongoStorageContainer : IStorageContainer
    {
        ILoggerFactory loggerFactory; IMongoStorage mongoStorage;
        public MongoStorageContainer(ILoggerFactory loggerFactory, IMongoStorage mongoStorage)
        {
            this.loggerFactory = loggerFactory;
            this.mongoStorage = mongoStorage;
        }
        private MongoGrainConfig GetESMongoInfo<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            if (grain is IMongoGrain mongoGrain && mongoGrain.ESMongoInfo != null)
            {
#pragma warning disable CS4014 // 由于此调用不会等待，因此在调用完成前将继续执行当前方法
                mongoGrain.ESMongoInfo.CreateIndex(mongoStorage);
#pragma warning restore CS4014 // 由于此调用不会等待，因此在调用完成前将继续执行当前方法
                return mongoGrain.ESMongoInfo;
            }
            return null;
        }
        public IEventStorage<K> GetEventStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var mongoInfo = GetESMongoInfo<K, S>(type, grain);
            if (mongoInfo != null)
            {
                return new MongoEventStorage<K>(mongoStorage, loggerFactory.CreateLogger<MongoEventStorage<K>>(), mongoInfo);
            }
            else
                throw new Exception("Not find MongoGrainConfig");
        }
        public IStateStorage<S, K> GetStateStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var mongoInfo = GetESMongoInfo<K, S>(type, grain);
            if (mongoInfo != null)
            {
                return new MongoStateStorage<S, K>(mongoStorage, mongoInfo.EventDataBase, mongoInfo.SnapshotCollection);
            }
            else
                throw new Exception("Not find MongoGrainConfig");
        }
    }
}
