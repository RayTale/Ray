using Orleans;
using Ray.Core.EventSourcing;
using System;
using System.Collections.Concurrent;

namespace Ray.PostgresqlES
{
    public class StorageContainer : IStorageContainer
    {
        protected static ConcurrentDictionary<Type, SqlTable> mongoAttrDict = new ConcurrentDictionary<Type, SqlTable>();

        static Type eventStorageType = typeof(SqlTable);
        private SqlTable GetTableInfo<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            if (grain is ISqlGrain sqlGrain && sqlGrain.ESSQLTable != null)
            {
                return sqlGrain.ESSQLTable;
            }
            return null;
        }
        public IEventStorage<K> GetEventStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var table = GetTableInfo<K, S>(type, grain);
            if (table != null)
            {
                return new EventStorage<K>(table);
            }
            else
                throw new Exception("not find sqltable info");
        }

        public IStateStorage<S, K> GetStateStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var table = GetTableInfo<K, S>(type, grain);
            if (table != null)
            {
                return new StateStorage<S, K>(table, table.SnapshotTable);
            }
            else
                throw new Exception("not find state table ");
        }

        public IStateStorage<S, K> GetToDbStateStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var table = GetTableInfo<K, S>(type, grain);
            if (table != null)
            {
                return new StateStorage<S, K>(table, table.ToDbSnapshotTable);
            }
            else
                throw new Exception("not find todbstate table ");
        }
    }
}
