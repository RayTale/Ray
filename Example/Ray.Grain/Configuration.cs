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
            followUnitContainer.Register(FollowUnitWithLong.From<Account>(serviceProvider).
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
            serviceCollection.PostConfigure<ArchiveOptions<Account>>(options =>
            {
                options.On = true;
                options.EventClearOn = false;
                options.IntervalMilliSeconds = 60 * 1000;
                options.IntervalVersion = 500;
            });
        }
        public void ConfigureBase(IServiceCollection serviceCollection)
        {
            serviceCollection.PostConfigure<CoreOptions<Account>>(options =>
            {
                options.ClearEventWhenOver = true;
                options.PriorityAsyncEventBus = true;
            });
        }
    }
}
