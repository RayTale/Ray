using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Ray.Core.Serialization;
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
        public async ValueTask<IEventStorage<PrimaryKey>> CreateEventStorage<PrimaryKey>(Grain grain, PrimaryKey grainId)
        {
            var grainType = grain.GetType();
            if (configureContainer.TryGetValue(grainType, out var value) &&
                value is ConfigureBuilderWrapper<PrimaryKey, StorageConfig, ConfigParameter> builder)
            {
                var dictKey = builder.Parameter.Singleton ? grainType.FullName : $"{grainType.FullName}-{grainId.ToString()}";
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
                     return new EventStorage<PrimaryKey>(serviceProvider, configTask.Result);
                 });
                return storage as EventStorage<PrimaryKey>;
            }
            else
            {
                throw new NotImplementedException($"{nameof(ConfigureBuilderWrapper<PrimaryKey, StorageConfig, ConfigParameter>)} of {grainType.FullName}");
            }
        }
        readonly ConcurrentDictionary<string, object> stateStorageDict = new ConcurrentDictionary<string, object>();
        public async ValueTask<ISnapshotStorage<PrimaryKey, State>> CreateSnapshotStorage<PrimaryKey, State>(Grain grain, PrimaryKey grainId)
            where State : class, new()
        {
            var grainType = grain.GetType();
            if (configureContainer.TryGetValue(grainType, out var value) &&
                value is ConfigureBuilderWrapper<PrimaryKey, StorageConfig, ConfigParameter> builder)
            {
                var dictKey = builder.Parameter.Singleton ? grainType.FullName : $"{grainType.FullName}-{grainId.ToString()}";
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
                   return new SnapshotStorage<PrimaryKey, State>(serviceProvider.GetService<ISerializer>(), configTask.Result);
               });
                return storage as ISnapshotStorage<PrimaryKey, State>;
            }
            else
            {
                throw new NotImplementedException($"{nameof(ConfigureBuilderWrapper<PrimaryKey, StorageConfig, ConfigParameter>)} of {grainType.FullName}");
            }
        }
        readonly ConcurrentDictionary<string, object> ArchiveStorageDict = new ConcurrentDictionary<string, object>();
        public async ValueTask<IArchiveStorage<PrimaryKey, State>> CreateArchiveStorage<PrimaryKey, State>(Grain grain, PrimaryKey grainId)
             where State : class, new()
        {
            var grainType = grain.GetType();
            if (configureContainer.TryGetValue(grainType, out var value) &&
                value is ConfigureBuilderWrapper<PrimaryKey, StorageConfig, ConfigParameter> builder)
            {
                var dictKey = builder.Parameter.Singleton ? grainType.FullName : $"{grainType.FullName}-{grainId.ToString()}";
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
                var storage = ArchiveStorageDict.GetOrAdd(dictKey, key =>
                {
                    return new ArchiveStorage<PrimaryKey, State>(serviceProvider.GetService<ISerializer>(), configTask.Result);
                });
                return storage as IArchiveStorage<PrimaryKey, State>;
            }
            else
            {
                throw new NotImplementedException($"{nameof(ConfigureBuilderWrapper<PrimaryKey, StorageConfig, ConfigParameter>)} of {grainType.FullName}");
            }
        }
        readonly ConcurrentDictionary<string, object> FollowSnapshotStorageDict = new ConcurrentDictionary<string, object>();
        public async ValueTask<IFollowSnapshotStorage<PrimaryKey>> CreateFollowSnapshotStorage<PrimaryKey>(Grain grain, PrimaryKey grainId)
        {
            var grainType = grain.GetType();
            if (configureContainer.TryGetValue(grainType, out var value) &&
                value is ConfigureBuilderWrapper<PrimaryKey, StorageConfig, ConfigParameter> builder)
            {
                var dictKey = builder.Parameter.Singleton ? grainType.FullName : $"{grainType.FullName}-{grainId.ToString()}";
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
                var storage = FollowSnapshotStorageDict.GetOrAdd(dictKey, key =>
                {
                    return new FollowSnapshotStorage<PrimaryKey>(configTask.Result);
                });
                return storage as FollowSnapshotStorage<PrimaryKey>;
            }
            else
            {
                throw new NotImplementedException($"{nameof(ConfigureBuilderWrapper<PrimaryKey, StorageConfig, ConfigParameter>)} of {grainType.FullName}");
            }
        }
    }
}
