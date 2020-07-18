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
        private readonly Func<IServiceProvider, PrimaryKey, Parameter, Config> generator;
        private readonly Dictionary<Type, Func<IServiceProvider, PrimaryKey, Parameter, FollowConfig>> followConfigGeneratorDict = new Dictionary<Type, Func<IServiceProvider, PrimaryKey, Parameter, FollowConfig>>();
        private readonly ConcurrentDictionary<Type, Task<FollowConfig>> singletonObserverConfigDict = new ConcurrentDictionary<Type, Task<FollowConfig>>();
        private IStorageOptions config;

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
            this.Observe(typeof(Follow), generator);
        }

        protected void Observe(Type type, Func<IServiceProvider, PrimaryKey, Parameter, FollowConfig> generator)
        {
            this.followConfigGeneratorDict.Add(type, generator);
        }

        private readonly SemaphoreSlim seamphore = new SemaphoreSlim(1, 1);

        public async ValueTask<IStorageOptions> GetConfig(IServiceProvider serviceProvider, PrimaryKey primaryKey)
        {
            if (this.parameter.Singleton)
            {
                if (this.config is null)
                {
                    await this.seamphore.WaitAsync();
                    try
                    {
                        if (this.config is null)
                        {
                            this.config = this.generator(serviceProvider, primaryKey, this.parameter);
                            this.config.Singleton = this.parameter.Singleton;
                            var initTask = this.config.Build();
                            if (!initTask.IsCompletedSuccessfully)
                            {
                                await initTask;
                            }
                        }
                    }
                    finally
                    {
                        this.seamphore.Release();
                    }
                }

                return this.config;
            }
            else
            {
                var newConfig = this.generator(serviceProvider, primaryKey, this.parameter);
                newConfig.Singleton = this.parameter.Singleton;
                var initTask = newConfig.Build();
                if (!initTask.IsCompletedSuccessfully)
                {
                    await initTask;
                }

                return newConfig;
            }
        }

        public async ValueTask<IObserverStorageOptions> GetObserverConfig(IServiceProvider serviceProvider, Type followGrainType, PrimaryKey primaryKey)
        {
            if (this.parameter.Singleton)
            {
                return await this.singletonObserverConfigDict.GetOrAdd(followGrainType, async key =>
                {
                    if (this.followConfigGeneratorDict.TryGetValue(followGrainType, out var followGenerator))
                    {
                        var task = this.GetConfig(serviceProvider, primaryKey);
                        if (!task.IsCompleted)
                        {
                            await task;
                        }

                        var followConfig = followGenerator(serviceProvider, primaryKey, this.parameter);
                        followConfig.Config = task.Result;
                        var initTask = followConfig.Build();
                        if (!initTask.IsCompletedSuccessfully)
                        {
                            await initTask;
                        }

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
                if (this.followConfigGeneratorDict.TryGetValue(followGrainType, out var followGenerator))
                {
                    var task = this.GetConfig(serviceProvider, primaryKey);
                    if (!task.IsCompleted)
                    {
                        await task;
                    }

                    var followConfig = followGenerator(serviceProvider, primaryKey, this.parameter);
                    followConfig.Config = task.Result;
                    var initTask = followConfig.Build();
                    if (!initTask.IsCompletedSuccessfully)
                    {
                        await initTask;
                    }

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
