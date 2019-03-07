using System;
using Ray.Core.Storage;
using Ray.Storage.Mongo.Configuration;

namespace Ray.Storage.Mongo
{
    public class MongoConfigureBuilder<PrimaryKey, Grain> : ConfigureBuilder<PrimaryKey, Grain, StorageOptions, FollowStorageOptions, DefaultConfigParameter>
    {
        public MongoConfigureBuilder(Func<IServiceProvider, PrimaryKey, DefaultConfigParameter, StorageOptions> generator, bool singleton = true) : base(generator, new DefaultConfigParameter(singleton))
        {
        }

        public override Type StorageFactory => typeof(StorageFactory);

        public MongoConfigureBuilder<PrimaryKey, Grain> Follow<FollowGrain>(string followName = null)
            where FollowGrain : Orleans.Grain
        {
            Follow<FollowGrain>((provider, id, parameter) => new FollowStorageOptions { FollowName = followName });
            return this;
        }
    }
}
