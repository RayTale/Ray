using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core.Event;
using Ray.Core.Serialization;
using Ray.Core.State;
using Ray.Core.Storage;

namespace Ray.Storage.MongoDB
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
        public async ValueTask<IEventStorage<K>> CreateEventStorage<K>(Grain grain, K grainId)
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
                    if (!task.IsCompletedSuccessfully)
                        await task;
                    return newConfig;
                });
                if (!configTask.IsCompletedSuccessfully)
                    await configTask;
                var storage = eventStorageDict.GetOrAdd(dictKey, key =>
                 {
                     return new MongoEventStorage<K>(serviceProvider, configTask.Result);
                 });
                return storage as MongoEventStorage<K>;
            }
            else
            {
                throw new NotImplementedException($"{nameof(ConfigureBuilderWrapper<K, StorageConfig, ConfigParameter>)} of {grainType.FullName}");
            }
        }
        readonly ConcurrentDictionary<string, object> stateStorageDict = new ConcurrentDictionary<string, object>();
        public async ValueTask<ISnapshotStorage<K, S>> CreateSnapshotStorage<K, S>(Grain grain, K grainId)
            where S : class, new()
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
                    if (!task.IsCompletedSuccessfully)
                        await task;
                    return newConfig;
                });
                if (!configTask.IsCompletedSuccessfully)
                    await configTask;
                var storage = stateStorageDict.GetOrAdd(dictKey, key =>
               {
                   return new MongoStateStorage<K, S>(serviceProvider.GetService<ISerializer>(), configTask.Result);
               });
                return storage as MongoStateStorage<K, S>;
            }
            else
            {
                throw new NotImplementedException($"{nameof(ConfigureBuilderWrapper<K, StorageConfig, ConfigParameter>)} of {grainType.FullName}");
            }
        }

        public ValueTask<IArchiveStorage<K, S>> CreateArchiveStorage<K, S>(Grain grain, K grainId)
            where S : class, new()
        {
            //TODO 
            throw new NotImplementedException();
        }

        public ValueTask<IFollowSnapshotStorage<PrimaryKey>> CreateFollowSnapshotStorage<PrimaryKey>(Grain grain, PrimaryKey grainId)
        {
            throw new NotImplementedException();
        }
    }
}
