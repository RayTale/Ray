using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core.Event;
using Ray.Core.Serialization;
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
        public async ValueTask<IEventStorage<K, E>> CreateEventStorage<K, E>(Grain grain, K grainId)
             where E : IEventBase<K>
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
                     return new SqlEventStorage<K, E>(serviceProvider, configTask.Result);
                 });
                return storage as SqlEventStorage<K, E>;
            }
            else
            {
                throw new NotImplementedException($"{nameof(ConfigureBuilderWrapper<K, StorageConfig, ConfigParameter>)} of {grainType.FullName}");
            }
        }
        readonly ConcurrentDictionary<string, object> stateStorageDict = new ConcurrentDictionary<string, object>();
        public async ValueTask<ISnapshotStorage<K, S, B>> CreateSnapshotStorage<K, S, B>(Grain grain, K grainId)
            where S : class, IState<K, B>, new()
            where B : ISnapshot<K>, new()
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
                   return new SqlStateStorage<K, S, B>(serviceProvider.GetService<ISerializer>(), configTask.Result);
               });
                return storage as SqlStateStorage<K, S, B>;
            }
            else
            {
                throw new NotImplementedException($"{nameof(ConfigureBuilderWrapper<K, StorageConfig, ConfigParameter>)} of {grainType.FullName}");
            }
        }

        ValueTask<IArchiveStorage<K, S, B>> IStorageFactory.CreateArchiveStorage<K, S, B>(Grain grain, K grainId)
        {
            //TODO
            throw new NotImplementedException();
        }
    }
}
