using System;
using Orleans;
using Ray.Core.Storage;

namespace Ray.Storage.PostgreSQL
{
    public class SQLConfigureBuilder<PrimaryKey> : ConfigureBuilder<PrimaryKey, StorageConfig, ConfigParameter>
    {
        readonly bool singleton;
        public SQLConfigureBuilder(Func<Grain, PrimaryKey, ConfigParameter, StorageConfig> generator, bool singleton = true) : base(generator)
        {
            this.singleton = singleton;
        }
        public SQLConfigureBuilder<PrimaryKey> Bind<Grain>()
        {
            AllotTo<Grain>(new ConfigParameter(singleton, false));
            return this;
        }
        public SQLConfigureBuilder<PrimaryKey> Follow<Grain>(string followName = null)
        {
            AllotTo<Grain>(new ConfigParameter(singleton, true, followName));
            return this;
        }
    }
}
