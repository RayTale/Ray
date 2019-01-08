using System;
using Orleans;
using Ray.Core.Storage;

namespace Ray.Storage.MongoDB
{
    public class MongoConfigureBuilder<K> : ConfigureBuilder<K, StorageConfig, ConfigParameter>
    {
        readonly bool staticByType;
        public MongoConfigureBuilder(Func<Grain, K, ConfigParameter, StorageConfig> generator, bool staticByType = true) : base(generator)
        {
            this.staticByType = staticByType;
        }
        public MongoConfigureBuilder<K> BindTo<T>(string snapshotCollection = null)
        {
            BindTo<K>(new ConfigParameter(staticByType, snapshotCollection));
            return this;
        }
    }
}
