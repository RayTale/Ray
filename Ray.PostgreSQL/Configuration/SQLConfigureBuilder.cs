using System;
using Ray.Core.Storage;

namespace Ray.Storage.PostgreSQL
{
    public class SQLConfigureBuilder<PrimaryKey, Grain> : ConfigureBuilder<PrimaryKey, Grain, StorageConfig, FollowStorageConfig, DefaultConfigParameter>
    {
        public SQLConfigureBuilder(Func<IServiceProvider, PrimaryKey, DefaultConfigParameter, StorageConfig> generator, bool singleton = true) :
            base(generator, new DefaultConfigParameter(singleton))
        {
        }

        public override Type StorageFactory => typeof(StorageFactory);

        public SQLConfigureBuilder<PrimaryKey, Grain> Follow<FollowGrain>(string followName = null)
            where FollowGrain : Orleans.Grain
        {
            Follow<FollowGrain>((provider, id, parameter) => new FollowStorageConfig(provider) { FollowName = followName });
            return this;
        }
    }
}
