using Orleans;
using Ray.Core.EventSourcing;
using System;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Threading;
using System.Linq;

namespace Ray.PostgreSQL
{
    public class SqlStorageContainer : IStorageContainer
    {
        readonly Timer monitorTimer;
        public SqlStorageContainer()
        {
            monitorTimer = new Timer(state =>
            {
                Task.WhenAll(eventStorageDict.Values.Select(storage =>
                {
                    if (storage is IEventFlowStorage flow)
                    {
                        return flow.TriggerFlowProcess();
                    }
                    return Task.CompletedTask;
                })).Wait();
            }, null, 10 * 1000, 20 * 1000);
        }
        private async Task<SqlGrainConfig> GetTableInfo(Type type, Grain grain)
        {
            if (grain is ISqlGrain sqlGrain && sqlGrain.GrainConfig != null)
            {
                await sqlGrain.GrainConfig.Build();
                return sqlGrain.GrainConfig;
            }
            return null;
        }
        ConcurrentDictionary<string, object> eventStorageDict = new ConcurrentDictionary<string, object>();
        public async ValueTask<IEventStorage<K>> GetEventStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var table = await GetTableInfo(type, grain);
            if (table != null)
            {
                if (!eventStorageDict.TryGetValue(table.EventFlowKey, out var storage))
                {
                    storage = new SqlEventStorage<K>(table);
                    if (!eventStorageDict.TryAdd(table.EventFlowKey, storage))
                        storage = eventStorageDict[table.EventFlowKey];
                }
                return storage as IEventStorage<K>;
            }
            else
                throw new Exception("not find sqltable info");
        }

        public async ValueTask<IStateStorage<S, K>> GetStateStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new()
        {
            var table = await GetTableInfo(type, grain);
            if (table != null)
            {
                return new SqlStateStorage<S, K>(table);
            }
            else
                throw new Exception("not find sqltable info");
        }
    }
}
