using System;
using System.Collections.Generic;
using Orleans;

namespace Ray.Core.Storage
{
    public class ConfigureBuilder<PrimaryKey, Config, Parameter> : IConfigureBuilder<PrimaryKey, Config, Parameter>
    {
        readonly List<(Type type, Parameter parameter)> bindList = new List<(Type type, Parameter parameter)>();
        readonly Func<Grain, PrimaryKey, Parameter, Config> generator;
        public ConfigureBuilder(
            Func<Grain, PrimaryKey, Parameter, Config> generator)
        {
            this.generator = generator;
        }

        public IConfigureBuilder<PrimaryKey, Config, Parameter> AllotTo<Grain>(Parameter parameter = default)
        {
            bindList.Add((typeof(Grain), parameter));
            return this;
        }

        public void Complete(IConfigureBuilderContainer container)
        {
            foreach (var (type, parameter) in bindList)
            {
                container.Register(type, new ConfigureBuilderWrapper<PrimaryKey, Config, Parameter>(generator, parameter));
            }
        }
    }
}
