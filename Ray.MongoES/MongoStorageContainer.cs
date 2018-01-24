using Orleans;
using Ray.Core.EventSourcing;
using System;
using System.Collections.Concurrent;

namespace Ray.MongoES
{
    public class MongoStorageContainer : IStorageContainer
    {
        protected static ConcurrentDictionary<Type, MongoStorageAttribute> mongoAttrDict = new ConcurrentDictionary<Type, MongoStorageAttribute>();

        static Type eventStorageType = typeof(MongoStorageAttribute);
        private static MongoStorageAttribute LoadMongoAttr(Type type)
        {
            var mongoStorageAttributes = type.GetCustomAttributes(eventStorageType, true);
            if (mongoStorageAttributes.Length > 0)
            {
                return mongoStorageAttributes[0] as MongoStorageAttribute;
            }
            return new MongoStorageAttribute("EventSourcing", type.FullName);
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
                mongoAttrDict.TryAdd(type, _mongoInfo);
            }
            return _mongoInfo;
        }
        public IEventStorage<K> GetEventStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var mongoInfo = GetESMongoInfo<K, S>(type, grain);
            if (mongoInfo != null)
            {
                return new MongoEventStorage<K>(mongoInfo);
            }
            else
                throw new Exception("not find MongoStorageAttribute");
        }
        public IStateStorage<S, K> GetStateStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var mongoInfo = GetESMongoInfo<K, S>(type, grain);
            if (mongoInfo != null)
            {
                return new MongoStateStorage<S, K>(mongoInfo.EventDataBase, mongoInfo.SnapshotCollection);
            }
            else
                throw new Exception("not find MongoStorageAttribute");
        }
        public IStateStorage<S, K> GetToDbStateStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var mongoInfo = GetESMongoInfo<K, S>(type, grain);
            if (mongoInfo != null)
            {
                return new MongoStateStorage<S, K>(mongoInfo.EventDataBase, mongoInfo.ToDbSnapshotCollection);
            }
            else
                throw new Exception("not find MongoStorageAttribute");
        }

    }
}
