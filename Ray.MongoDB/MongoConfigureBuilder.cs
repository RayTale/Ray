using System;
using Orleans;
using Ray.Core.Storage;

namespace Ray.Storage.MongoDB
{
    public class MongoConfigureBuilder<PrimaryKey> : ConfigureBuilder<PrimaryKey, StorageConfig, ConfigParameter>
    {
        readonly bool singleton;
        public MongoConfigureBuilder(Func<Grain, PrimaryKey, ConfigParameter, StorageConfig> generator, bool singleton = true) : base(generator)
        {
            this.singleton = singleton;
        }
        public MongoConfigureBuilder<PrimaryKey> Bind<Grain>()
        {
            AllotTo<Grain>(new ConfigParameter(singleton, false));
            return this;
        }
        public MongoConfigureBuilder<PrimaryKey> Follow<Grain>(string followName = null)
        {
            AllotTo<Grain>(new ConfigParameter(singleton, true, followName));
            return this;
        }
    }
}
