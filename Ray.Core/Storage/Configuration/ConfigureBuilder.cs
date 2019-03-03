using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public abstract class ConfigureBuilder<PrimaryKey, Grain, Config, FollowConfig, Parameter> : IConfigureBuilder<PrimaryKey, Grain>
         where Config : IStorageConfig
         where FollowConfig : IFollowStorageConfig
         where Parameter : IConfigParameter
    {
        protected readonly Parameter parameter;
        readonly Func<IServiceProvider, PrimaryKey, Parameter, Config> generator;
        readonly Dictionary<Type, Func<IServiceProvider, PrimaryKey, Parameter, FollowConfig>> FollowConfigGeneratorDict = new Dictionary<Type, Func<IServiceProvider, PrimaryKey, Parameter, FollowConfig>>();
        readonly ConcurrentDictionary<Type, Task<FollowConfig>> SingletonFollowConfigDict = new ConcurrentDictionary<Type, Task<FollowConfig>>();
        IStorageConfig config;
        public ConfigureBuilder(
            Func<IServiceProvider, PrimaryKey, Parameter, Config> generator,
            Parameter parameter)
        {
            this.generator = generator;
            this.parameter = parameter;
        }
        public abstract Type StorageFactory { get; }

        protected void Follow<Follow>(Func<IServiceProvider, PrimaryKey, Parameter, FollowConfig> generator)
        {
            FollowConfigGeneratorDict.Add(typeof(Follow), generator);
        }
        readonly SemaphoreSlim seamphore = new SemaphoreSlim(1, 1);
        public async ValueTask<IStorageConfig> GetConfig(IServiceProvider serviceProvider, PrimaryKey primaryKey)
        {
            if (parameter.Singleton)
            {
                if (config == default)
                {
                    await seamphore.WaitAsync();
                    try
                    {
                        if (config == default)
                        {
                            config = generator(serviceProvider, primaryKey, parameter);
                            config.Singleton = parameter.Singleton;
                            var initTask = config.Init();
                            if (!initTask.IsCompletedSuccessfully)
                                await initTask;
                        }
                    }
                    finally
                    {
                        seamphore.Release();
                    }
                }
                return config;
            }
            else
            {
                var newConfig = generator(serviceProvider, primaryKey, parameter);
                newConfig.Singleton = parameter.Singleton;
                var initTask = newConfig.Init();
                if (!initTask.IsCompletedSuccessfully)
                    await initTask;
                return newConfig;
            }
        }
        public async ValueTask<IFollowStorageConfig> GetFollowConfig(IServiceProvider serviceProvider, Type followGrainType, PrimaryKey primaryKey)
        {
            if (parameter.Singleton)
            {
                return await SingletonFollowConfigDict.GetOrAdd(followGrainType, async key =>
                {
                    if (FollowConfigGeneratorDict.TryGetValue(followGrainType, out var followGenerator))
                    {
                        var task = GetConfig(serviceProvider, primaryKey);
                        if (!task.IsCompleted)
                            await task;
                        var followConfig = followGenerator(serviceProvider, primaryKey, parameter);
                        followConfig.Config = task.Result;
                        var initTask = followConfig.Init();
                        if (!initTask.IsCompletedSuccessfully)
                            await initTask;
                        return followConfig;
                    }
                    else
                    {
                        throw new NotImplementedException(followGrainType.FullName);
                    }
                });
            }
            else
            {
                if (FollowConfigGeneratorDict.TryGetValue(followGrainType, out var followGenerator))
                {
                    var task = GetConfig(serviceProvider, primaryKey);
                    if (!task.IsCompleted)
                        await task;
                    var followConfig = followGenerator(serviceProvider, primaryKey, parameter);
                    followConfig.Config = task.Result;
                    var initTask = followConfig.Init();
                    if (!initTask.IsCompletedSuccessfully)
                        await initTask;
                    return followConfig;
                }
                else
                {
                    throw new NotImplementedException(followGrainType.FullName);
                }
            }
        }
    }
}
