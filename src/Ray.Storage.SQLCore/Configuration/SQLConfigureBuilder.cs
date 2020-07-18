using System;
using Ray.Core.Storage;

namespace Ray.Storage.SQLCore.Configuration
{
    public class SQLConfigureBuilder<TFactory, TPrimaryKey, TGrain> :
        ConfigureBuilder<TPrimaryKey, TGrain, StorageOptions, ObserverStorageOptions, DefaultConfigParameter>
        where TFactory : IStorageFactory
    {
        public SQLConfigureBuilder(
            Func<IServiceProvider, TPrimaryKey, DefaultConfigParameter, StorageOptions> generator,
            bool singleton = true)
            : base(generator, new DefaultConfigParameter(singleton))
        {
        }

        public override Type StorageFactory => typeof(TFactory);

        public SQLConfigureBuilder<TFactory, TPrimaryKey, TGrain> Observe<TFollowGrain>(string observerName = null)
            where TFollowGrain : Orleans.Grain
        {
            this.Observe<TFollowGrain>((provider, id, parameter) => new ObserverStorageOptions { ObserverName = observerName });
            return this;
        }

        public SQLConfigureBuilder<TFactory, TPrimaryKey, TGrain> AutoRegistrationObserver()
        {
            foreach (var (type, observer) in Core.CoreExtensions.AllObserverAttribute)
            {
                if (observer.Observable == typeof(TGrain))
                {
                    this.Observe(
                        type,
                        (provider, id, parameter) => new ObserverStorageOptions { ObserverName = observer.Name });
                }
            }

            return this;
        }
    }
}