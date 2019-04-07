using System;
using Ray.Core;
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
        public SQLConfigureBuilder<Factory, PrimaryKey, Grain> Observe<FollowGrain>(string observerName = null)
            where FollowGrain : Orleans.Grain
        {
            Observe<FollowGrain>((provider, id, parameter) => new ObserverStorageOptions { ObserverName = observerName });
            return this;
        }
        public SQLConfigureBuilder<Factory, PrimaryKey, Grain> AutoRegistrationObserver()
        {
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                foreach (var type in assembly.GetTypes())
                {
                    foreach (var attribute in type.GetCustomAttributes(false))
                    {
                        if (attribute is ObserverAttribute observer && observer.Observable == typeof(Grain))
                        {
                            Observe(type, (provider, id, parameter) => new ObserverStorageOptions { ObserverName = observer.Name });
                        }
                    }
                }
            }
            return this;
        }
    }
}
