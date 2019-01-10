using System;
using System.Collections.Generic;
using Orleans;

namespace Ray.Core.Storage
{
    public class ConfigureBuilder<K, C, P> : IConfigureBuilder<K, C, P>
    {
        readonly List<(Type type, P parameter)> bindList = new List<(Type type, P parameter)>();
        readonly Func<Grain, K, P, C> generator;
        public ConfigureBuilder(
            Func<Grain, K, P, C> generator)
        {
            this.generator = generator;
        }

        public IConfigureBuilder<K, C, P> BindTo<T>(P parameter = default)
        {
            bindList.Add((typeof(T), parameter));
            return this;
        }

        public void Complete(IConfigureBuilderContainer container)
        {
            foreach (var (type, parameter) in bindList)
            {
                container.Register(type, new ConfigureBuilderWrapper<K, C, P>(generator, parameter));
            }
        }
    }
}
