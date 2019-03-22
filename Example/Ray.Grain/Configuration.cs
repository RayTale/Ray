using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core;
using Ray.Core.Abstractions;
using Ray.Core.Configuration;
using Ray.IGrains.Actors;

namespace Ray.Grain
{
    public class Configuration : IStartupConfig
    {
        public Task ConfigureFollowUnit(IServiceProvider serviceProvider, IObserverUnitContainer followUnitContainer)
        {
            followUnitContainer.Register(ObserverUnit<long>.From<Account>(serviceProvider).
                Observer(DefaultObserverGroup.primary, (client, id) => client.GetGrain<IAccountRep>(id)).
                ConcurrentObserver(DefaultObserverGroup.primary, (client, id) => client.GetGrain<IAccountFlow>(id)).
                ConcurrentObserver(DefaultObserverGroup.secondary, (client, id) => client.GetGrain<IAccountDb>(id)));
            return Task.CompletedTask;
        }
        public void Configure(IServiceCollection serviceCollection)
        {
            ConfigureArchive(serviceCollection);
            ConfigureBase(serviceCollection);
        }
        public void ConfigureArchive(IServiceCollection serviceCollection)
        {
            serviceCollection.Configure<ArchiveOptions>(typeof(Account).FullName, options =>
            {
                options.On = true;
                options.SecondsInterval = 60;
                options.VersionInterval = 500;
            });
        }
        public void ConfigureBase(IServiceCollection serviceCollection)
        {
            serviceCollection.Configure<CoreOptions>(typeof(Account).FullName, options =>
            {
                options.ArchiveEventOnOver = true;
                options.PriorityAsyncEventBus = true;
            });
        }
    }
}
