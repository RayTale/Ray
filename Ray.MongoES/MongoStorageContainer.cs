using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.EventSourcing;
using System;
using System.Collections.Concurrent;

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
        protected static ConcurrentDictionary<Type, MongoStorageAttribute> mongoAttrDict = new ConcurrentDictionary<Type, MongoStorageAttribute>();

        static Type eventStorageType = typeof(MongoStorageAttribute);
        private MongoStorageAttribute LoadMongoAttr(Type type)
        {
            var mongoStorageAttributes = type.GetCustomAttributes(eventStorageType, true);
            if (mongoStorageAttributes.Length > 0)
            {
                return mongoStorageAttributes[0] as MongoStorageAttribute;
            }
            else
                throw new Exception("Not find MongoStorageAttribute");
        }
        private MongoStorageAttribute GetESMongoInfo<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            if (grain is IMongoGrain mongoGrain && mongoGrain.ESMongoInfo != null)
            {
                return mongoGrain.ESMongoInfo;
            }
            if (!mongoAttrDict.TryGetValue(type, out var _mongoInfo))
            {
                _mongoInfo = LoadMongoAttr(type);
#pragma warning disable CS4014 // 由于此调用不会等待，因此在调用完成前将继续执行当前方法
                _mongoInfo.CreateCollectionIndex(mongoStorage);//创建分表索引
                _mongoInfo.CreateStateIndex(mongoStorage);//创建快照索引
#pragma warning restore CS4014 // 由于此调用不会等待，因此在调用完成前将继续执行当前方法
                mongoAttrDict.TryAdd(type, _mongoInfo);
            }
            return _mongoInfo;
        }
        public IEventStorage<K> GetEventStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var mongoInfo = GetESMongoInfo<K, S>(type, grain);
            if (mongoInfo != null)
            {
                return new MongoEventStorage<K>(mongoStorage, loggerFactory.CreateLogger<MongoEventStorage<K>>(), mongoInfo);
            }
            else
                throw new Exception("Not find MongoStorageAttribute");
        }
        public IStateStorage<S, K> GetStateStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var mongoInfo = GetESMongoInfo<K, S>(type, grain);
            if (mongoInfo != null)
            {
                return new MongoStateStorage<S, K>(mongoStorage, mongoInfo.EventDataBase, mongoInfo.SnapshotCollection);
            }
            else
                throw new Exception("Not find MongoStorageAttribute");
        }
    }
}
