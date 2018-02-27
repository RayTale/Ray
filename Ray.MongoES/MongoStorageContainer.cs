using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.EventSourcing;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Ray.MongoES
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
            return new MongoStorageAttribute("EventSourcing", type.FullName);
        }
        private async Task<MongoStorageAttribute> GetESMongoInfo<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            if (grain is IMongoGrain mongoGrain && mongoGrain.ESMongoInfo != null)
            {
                return mongoGrain.ESMongoInfo;
            }
            if (!mongoAttrDict.TryGetValue(type, out var _mongoInfo))
            {
                _mongoInfo = LoadMongoAttr(type);
                await _mongoInfo.CreateCollectionIndex(mongoStorage);//创建分表索引
                await _mongoInfo.CreateStateIndex(mongoStorage);//创建快照索引
                mongoAttrDict.TryAdd(type, _mongoInfo);
            }
            return _mongoInfo;
        }
        public IEventStorage<K> GetEventStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var mongoInfo = GetESMongoInfo<K, S>(type, grain).GetAwaiter().GetResult();
            if (mongoInfo != null)
            {
                return new MongoEventStorage<K>(mongoStorage, loggerFactory.CreateLogger<MongoEventStorage<K>>(), mongoInfo);
            }
            else
                throw new Exception("not find MongoStorageAttribute");
        }
        public IStateStorage<S, K> GetStateStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var mongoInfo = GetESMongoInfo<K, S>(type, grain).GetAwaiter().GetResult();
            if (mongoInfo != null)
            {
                return new MongoStateStorage<S, K>(mongoStorage, mongoInfo.EventDataBase, mongoInfo.SnapshotCollection);
            }
            else
                throw new Exception("not find MongoStorageAttribute");
        }
    }
}
