﻿using System;
using Ray.Core.Storage;

namespace Ray.Storage.SQLCore.Configuration
{
    public class SQLConfigureBuilder<Factory, PrimaryKey, Grain> :
        ConfigureBuilder<PrimaryKey, Grain, StorageOptions, ObserverStorageOptions, DefaultConfigParameter>
        where Factory : IStorageFactory
    {
        public SQLConfigureBuilder(Func<IServiceProvider, PrimaryKey, DefaultConfigParameter, StorageOptions> generator, bool singleton = true) :
            base(generator, new DefaultConfigParameter(singleton))
        {
        }
        public override Type StorageFactory => typeof(Factory);
        public SQLConfigureBuilder<Factory, PrimaryKey, Grain> Observe<FollowGrain>(string followName = null)
            where FollowGrain : Orleans.Grain
        {
            Observe<FollowGrain>((provider, id, parameter) => new ObserverStorageOptions { ObserverName = followName });
            return this;
        }
    }
}
