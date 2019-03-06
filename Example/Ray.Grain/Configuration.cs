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
        public Task ConfigureFollowUnit(IServiceProvider serviceProvider, IFollowUnitContainer followUnitContainer)
        {
            followUnitContainer.Register(LongFollowUnit.From<Account>(serviceProvider).
                Flow<IAccountRep>(DefaultFollowType.primary).
                ConcurrentFlow<IAccountFlow>(DefaultFollowType.primary).
                ConcurrentFlow<IAccountDb>(DefaultFollowType.secondary));
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
