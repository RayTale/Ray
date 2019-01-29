using System;
using Orleans;

namespace Ray.Core.Storage
{
    public class ConfigureBuilderWrapper<PrimaryKey, Config, ParameterType> : BaseConfigureBuilderWrapper
    {
        public ConfigureBuilderWrapper(Func<Grain, PrimaryKey, ParameterType, Config> generator, ParameterType parameter)
        {
            Generator = generator;
            Parameter = parameter;
            FactoryType = typeof(IBaseStorageFactory<Config>);
        }
        public ParameterType Parameter { get; }
        public Func<Grain, PrimaryKey, ParameterType, Config> Generator { get; }
    }
}
