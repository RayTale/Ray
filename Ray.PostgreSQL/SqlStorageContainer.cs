using Orleans;
using Ray.Core.EventSourcing;
using System;

namespace Ray.PostgreSQL
{
    public class SqlStorageContainer : IStorageContainer
    {
        private SqlGrainConfig GetTableInfo<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            if (grain is ISqlGrain sqlGrain && sqlGrain.GrainConfig != null)
            {
                return sqlGrain.GrainConfig;
            }
            return null;
        }
        public IEventStorage<K> GetEventStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var table = GetTableInfo<K, S>(type, grain);
            if (table != null)
            {
                return new SqlEventStorage<K>(table);
            }
            else
                throw new Exception("not find sqltable info");
        }

        public IStateStorage<S, K> GetStateStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var table = GetTableInfo<K, S>(type, grain);
            if (table != null)
            {
                return new SqlStateStorage<S, K>(table);
            }
            else
                throw new Exception("not find sqltable info");
        }
    }
}
