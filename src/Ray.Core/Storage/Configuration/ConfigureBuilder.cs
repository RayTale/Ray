using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public abstract class ConfigureBuilder<PrimaryKey, Grain, Config, FollowConfig, Parameter> : IConfigureBuilder<PrimaryKey, Grain>
         where Config : IStorageOptions
         where FollowConfig : IObserverStorageOptions
         where Parameter : IConfigParameter
    {
        protected readonly Parameter parameter;
        readonly Func<IServiceProvider, PrimaryKey, Parameter, Config> generator;
        readonly Dictionary<Type, Func<IServiceProvider, PrimaryKey, Parameter, FollowConfig>> followConfigGeneratorDict = new Dictionary<Type, Func<IServiceProvider, PrimaryKey, Parameter, FollowConfig>>();
        readonly ConcurrentDictionary<Type, Task<FollowConfig>> singletonObserverConfigDict = new ConcurrentDictionary<Type, Task<FollowConfig>>();
        IStorageOptions config;
        public ConfigureBuilder(
            Func<IServiceProvider, PrimaryKey, Parameter, Config> generator,
            Parameter parameter)
        {
            this.generator = generator;
            this.parameter = parameter;
        }
        public abstract Type StorageFactory { get; }

        protected void Observe<Follow>(Func<IServiceProvider, PrimaryKey, Parameter, FollowConfig> generator)
        {
            Observe(typeof(Follow), generator);
        }
        protected void Observe(Type type, Func<IServiceProvider, PrimaryKey, Parameter, FollowConfig> generator)
        {
            followConfigGeneratorDict.Add(type, generator);
        }
        readonly SemaphoreSlim seamphore = new SemaphoreSlim(1, 1);
        public async ValueTask<IStorageOptions> GetConfig(IServiceProvider serviceProvider, PrimaryKey primaryKey)
        {
            if (parameter.Singleton)
            {
                if (config is null)
                {
                    await seamphore.WaitAsync();
                    try
                    {
                        if (config is null)
                        {
                            config = generator(serviceProvider, primaryKey, parameter);
                            config.Singleton = parameter.Singleton;
                            var initTask = config.Build();
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
                var initTask = newConfig.Build();
                if (!initTask.IsCompletedSuccessfully)
                    await initTask;
                return newConfig;
            }
        }
        public async ValueTask<IObserverStorageOptions> GetObserverConfig(IServiceProvider serviceProvider, Type followGrainType, PrimaryKey primaryKey)
        {
            if (parameter.Singleton)
            {
                return await singletonObserverConfigDict.GetOrAdd(followGrainType, async key =>
                {
                    if (followConfigGeneratorDict.TryGetValue(followGrainType, out var followGenerator))
                    {
                        var task = GetConfig(serviceProvider, primaryKey);
                        if (!task.IsCompleted)
                            await task;
                        var followConfig = followGenerator(serviceProvider, primaryKey, parameter);
                        followConfig.Config = task.Result;
                        var initTask = followConfig.Build();
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
                if (followConfigGeneratorDict.TryGetValue(followGrainType, out var followGenerator))
                {
                    var task = GetConfig(serviceProvider, primaryKey);
                    if (!task.IsCompleted)
                        await task;
                    var followConfig = followGenerator(serviceProvider, primaryKey, parameter);
                    followConfig.Config = task.Result;
                    var initTask = followConfig.Build();
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
