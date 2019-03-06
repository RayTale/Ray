using System;
using Ray.Core.Storage;

namespace Ray.Storage.SQLCore.Configuration
{
    public class SQLConfigureBuilder<Factory, PrimaryKey, Grain> :
        ConfigureBuilder<PrimaryKey, Grain, StorageOptions, FollowStorageConfig, DefaultConfigParameter>
        where Factory : IStorageFactory
    {
        public SQLConfigureBuilder(Func<IServiceProvider, PrimaryKey, DefaultConfigParameter, StorageOptions> generator, bool singleton = true) :
            base(generator, new DefaultConfigParameter(singleton))
        {
        }
        public override Type StorageFactory => typeof(Factory);
        public SQLConfigureBuilder<Factory, PrimaryKey, Grain> Follow<FollowGrain>(string followName = null)
            where FollowGrain : Orleans.Grain
        {
            Follow<FollowGrain>((provider, id, parameter) => new FollowStorageConfig { FollowName = followName });
            return this;
        }
    }
}
