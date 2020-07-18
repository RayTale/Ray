using Ray.Core;
using Ray.Core.Storage;
using Ray.Storage.Mongo.Configuration;
using System;

namespace Ray.Storage.Mongo
{
    public class MongoConfigureBuilder<PrimaryKey, Grain> : ConfigureBuilder<PrimaryKey, Grain, StorageOptions, ObserverStorageOptions, DefaultConfigParameter>
    {
        public MongoConfigureBuilder(Func<IServiceProvider, PrimaryKey, DefaultConfigParameter, StorageOptions> generator, bool singleton = true) : base(generator, new DefaultConfigParameter(singleton))
        {
        }

        public override Type StorageFactory => typeof(StorageFactory);

        public MongoConfigureBuilder<PrimaryKey, Grain> Observe<FollowGrain>(string observerName = null)
            where FollowGrain : Orleans.Grain
        {
            Observe<FollowGrain>((provider, id, parameter) => new ObserverStorageOptions { ObserverName = observerName });
            return this;
        }
        public MongoConfigureBuilder<PrimaryKey, Grain> AutoRegistrationObserver()
        {
            foreach (var (type, observer) in CoreExtensions.AllObserverAttribute)
            {
                if (observer.Observable == typeof(Grain))
                {
                    Observe(type, (provider, id, parameter) => new ObserverStorageOptions { ObserverName = observer.Name });
                }
            }
            return this;
        }
    }
}
