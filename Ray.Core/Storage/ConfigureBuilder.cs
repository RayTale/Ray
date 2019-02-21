using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public abstract class ConfigureBuilder<PrimaryKey, Grain, Config, Parameter> : IConfigureBuilder<PrimaryKey, Grain>
         where Config : IStorageConfig
         where Parameter : IStorageConfigParameter
    {
        protected readonly Dictionary<Type, Parameter> ParameterDict = new Dictionary<Type, Parameter>();
        readonly Func<IServiceProvider, PrimaryKey, Parameter, Config> generator;
        readonly ConcurrentDictionary<Type, ValueTask<Config>> ConfigDict = new ConcurrentDictionary<Type, ValueTask<Config>>();
        public ConfigureBuilder(Func<IServiceProvider, PrimaryKey, Parameter, Config> generator)
        {
            this.generator = generator;
        }
        public abstract Type StorageFactory { get; }
        public IConfigureBuilder<PrimaryKey, Grain> Bind<T>(Parameter parameter = default)
            where T : Orleans.Grain
        {
            ParameterDict.Add(typeof(T), parameter);
            return this;
        }
        public async ValueTask<IStorageConfig> GetConfig(IServiceProvider serviceProvider, Type type, PrimaryKey primaryKey)
        {
            if (ParameterDict.TryGetValue(type, out var parameter))
            {
                if (parameter.Singleton)
                {
                    var configTask = ConfigDict.GetOrAdd(type, async key =>
                      {
                          var newConfig = generator(serviceProvider, primaryKey, parameter);
                          var InitTask = newConfig.Init();
                          if (!InitTask.IsCompletedSuccessfully)
                              await InitTask;
                          newConfig.Singleton = parameter.Singleton;
                          return newConfig;
                      });
                    if (!configTask.IsCompletedSuccessfully)
                        await configTask;
                    return configTask.Result;
                }
                else
                {
                    var newConfig = generator(serviceProvider, primaryKey, parameter);
                    var InitTask = newConfig.Init();
                    if (!InitTask.IsCompletedSuccessfully)
                        await InitTask;
                    newConfig.Singleton = parameter.Singleton;
                    return newConfig;
                }
            }
            else
            {
                throw new NotImplementedException(type.FullName);
            }
        }
    }
}
