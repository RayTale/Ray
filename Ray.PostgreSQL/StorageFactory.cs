using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Orleans;
using Ray.Core.State;
using Ray.Core.Storage;

namespace Ray.Storage.PostgreSQL
{
    public class StorageFactory : IBaseStorageFactory<StorageConfig>
    {
        readonly IServiceProvider serviceProvider;
        readonly IConfigureBuilderContainer configureContainer;
        readonly ConcurrentDictionary<string, ValueTask<StorageConfig>> grainConfigDict = new ConcurrentDictionary<string, ValueTask<StorageConfig>>();
        public StorageFactory(
            IServiceProvider serviceProvider,
            IConfigureBuilderContainer configureContainer)
        {
            this.serviceProvider = serviceProvider;
            this.configureContainer = configureContainer;
        }
        readonly ConcurrentDictionary<string, object> eventStorageDict = new ConcurrentDictionary<string, object>();
        public async ValueTask<IEventStorage<K>> CreateEventStorage<K, S>(Grain grain, K grainId)
             where S : class, IActorState<K>, new()
        {
            var grainType = grain.GetType();
            if (configureContainer.TryGetValue(grainType, out var value) &&
                value is ConfigureBuilderWrapper<K, StorageConfig, ConfigParameter> builder)
            {
                var dictKey = builder.Parameter.StaticByType ? grainType.FullName : $"{grainType.FullName}-{grainId.ToString()}";
                var configTask = grainConfigDict.GetOrAdd(dictKey, async key =>
                {
                    var newConfig = builder.Generator(grain, grainId, builder.Parameter);
                    var task = newConfig.Build();
                    if (!task.IsCompleted)
                        await task;
                    return newConfig;
                });
                if (!configTask.IsCompleted)
                    await configTask;
                var storage = eventStorageDict.GetOrAdd(dictKey, key =>
                 {
                     return new SqlEventStorage<K>(serviceProvider, configTask.Result);
                 });
                return storage as SqlEventStorage<K>;
            }
            else
            {
                throw new NotImplementedException($"{nameof(ConfigureBuilderWrapper<K, StorageConfig, ConfigParameter>)} of {grainType.FullName}");
            }
        }
        readonly ConcurrentDictionary<string, object> stateStorageDict = new ConcurrentDictionary<string, object>();
        public async ValueTask<IStateStorage<K, S>> CreateStateStorage<K, S>(Grain grain, K grainId)
            where S : class, IActorState<K>, new()
        {
            var grainType = grain.GetType();
            if (configureContainer.TryGetValue(grainType, out var value) &&
                value is ConfigureBuilderWrapper<K, StorageConfig, ConfigParameter> builder)
            {
                var dictKey = builder.Parameter.StaticByType ? grainType.FullName : $"{grainType.FullName}-{grainId.ToString()}";
                var configTask = grainConfigDict.GetOrAdd(dictKey, async key =>
                {
                    var newConfig = builder.Generator(grain, grainId, builder.Parameter);
                    var task = newConfig.Build();
                    if (!task.IsCompleted)
                        await task;
                    return newConfig;
                });
                if (!configTask.IsCompleted)
                    await configTask;
                var storage = stateStorageDict.GetOrAdd(dictKey, key =>
               {
                   return new SqlStateStorage<K, S>(configTask.Result);
               });
                return storage as SqlStateStorage<K, S>;
            }
            else
            {
                throw new NotImplementedException($"{nameof(ConfigureBuilderWrapper<K, StorageConfig, ConfigParameter>)} of {grainType.FullName}");
            }
        }
    }
}
