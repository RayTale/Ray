using Orleans;
using Ray.Core.EventSourcing;
using System;
using System.Threading.Tasks;

namespace Ray.PostgreSQL
{
    public class SqlStorageContainer : IStorageContainer
    {
        private async Task<SqlGrainConfig> GetTableInfo<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            if (grain is ISqlGrain sqlGrain && sqlGrain.GrainConfig != null)
            {
                await sqlGrain.GrainConfig.Build();
                return sqlGrain.GrainConfig;
            }
            return null;
        }
        public async ValueTask<IEventStorage<K>> GetEventStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var table = await GetTableInfo<K, S>(type, grain);
            if (table != null)
            {
                return new SqlEventStorage<K>(table);
            }
            else
                throw new Exception("not find sqltable info");
        }

        public async ValueTask<IStateStorage<S, K>> GetStateStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var table = await GetTableInfo<K, S>(type, grain);
            if (table != null)
            {
                return new SqlStateStorage<S, K>(table);
            }
            else
                throw new Exception("not find sqltable info");
        }
    }
}
