using System;
using Orleans;
using Ray.Core.Storage;

namespace Ray.Storage.MongoDB
{
    public class MongoConfigureBuilder<PrimaryKey> : ConfigureBuilder<PrimaryKey, StorageConfig, ConfigParameter>
    {
        readonly bool staticByType;
        public MongoConfigureBuilder(Func<Grain, PrimaryKey, ConfigParameter, StorageConfig> generator, bool staticByType = true) : base(generator)
        {
            this.staticByType = staticByType;
        }
        public MongoConfigureBuilder<PrimaryKey> AllotTo<Grain>(string snapshotCollection = null)
        {
            AllotTo<Grain>(new ConfigParameter(staticByType, snapshotCollection));
            return this;
        }
    }
}
