using Ray.Core.Storage;
using System;

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
        public SQLConfigureBuilder<Factory, PrimaryKey, Grain> Observe<FollowGrain>(string observerName = null)
            where FollowGrain : Orleans.Grain
        {
            Observe<FollowGrain>((provider, id, parameter) => new ObserverStorageOptions { ObserverName = observerName });
            return this;
        }
        public SQLConfigureBuilder<Factory, PrimaryKey, Grain> AutoRegistrationObserver()
        {
            foreach (var (type, observer) in Core.CoreExtensions.AllObserverAttribute)
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
