using System;
using Ray.Core.Storage;
using Ray.Storage.MongoDB.Configuration;

namespace Ray.Storage.MongoDB
{
    public class MongoConfigureBuilder<PrimaryKey, Grain> : ConfigureBuilder<PrimaryKey, Grain, StorageConfig, FollowStorageConfig, DefaultConfigParameter>
    {
        public MongoConfigureBuilder(Func<IServiceProvider, PrimaryKey, DefaultConfigParameter, StorageConfig> generator, bool singleton = true) : base(generator, new DefaultConfigParameter(singleton))
        {
        }

        public override Type StorageFactory => typeof(StorageFactory);

        public MongoConfigureBuilder<PrimaryKey, Grain> Follow<FollowGrain>(string followName = null)
            where FollowGrain : Orleans.Grain
        {
            Follow<FollowGrain>((provider, id, parameter) => new FollowStorageConfig { FollowName = followName });
            return this;
        }
    }
}
