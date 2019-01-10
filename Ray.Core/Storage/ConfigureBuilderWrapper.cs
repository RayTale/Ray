using System;
using Orleans;

namespace Ray.Core.Storage
{
    public class ConfigureBuilderWrapper<K, C, P>: BaseConfigureBuilderWrapper
    {
        public ConfigureBuilderWrapper(Func<Grain, K, P, C> generator, P parameter)
        {
            Generator = generator;
            Parameter = parameter;
            FactoryType = typeof(IBaseStorageFactory<C>);
        }
        public P Parameter { get; }
        public Func<Grain, K, P, C> Generator { get; }
    }
}
