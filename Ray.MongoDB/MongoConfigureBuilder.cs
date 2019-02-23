using System;
using Orleans;
using Ray.Core.Storage;

namespace Ray.Storage.MongoDB
{
    public class MongoConfigureBuilder<PrimaryKey, Grain> : ConfigureBuilder<PrimaryKey, Grain, StorageConfig, DefaultConfigParameter>
    {
        readonly bool singleton;
        public MongoConfigureBuilder(Func<IServiceProvider, PrimaryKey, DefaultConfigParameter, StorageConfig> generator, bool singleton = true) : base(generator)
        {
            this.singleton = singleton;
            ParameterDict.Add(typeof(Grain), new DefaultConfigParameter(singleton, false));
        }

        public override Type StorageFactory => typeof(StorageFactory);

        public MongoConfigureBuilder<PrimaryKey, Grain> Follow<T>(string followName = null)
            where T : Orleans.Grain
        {
            Bind<T>(new DefaultConfigParameter(singleton, true, followName));
            return this;
        }
    }
}
