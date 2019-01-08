using System;
using Orleans;
using Ray.Core.Storage;

namespace Ray.Storage.PostgreSQL
{
    public class SQLConfigureBuilder<K> : ConfigureBuilder<K, StorageConfig, ConfigParameter>
    {
        readonly bool staticByType;
        public SQLConfigureBuilder(Func<Grain, K, ConfigParameter, StorageConfig> generator, bool staticByType = true) : base(generator)
        {
            this.staticByType = staticByType;
        }
        public SQLConfigureBuilder<K> BindTo<T>(string snapshotTable = null)
        {
            BindTo<T>(new ConfigParameter(staticByType, snapshotTable));
            return this;
        }
    }
}
