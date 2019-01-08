using System;
using Orleans;

namespace Ray.Core.Storage
{
    public class ConfigureBuilderWrapper<K, C, P>
    {
        public ConfigureBuilderWrapper(Func<Grain, K, P, C> generator, P parameter)
        {
            Generator = generator;
            Parameter = parameter;
        }
        public P Parameter { get; }
        public Func<Grain, K, P, C> Generator { get; }
    }
}
