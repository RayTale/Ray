using System;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core;
using Ray.Core.Abstractions;
using Ray.Core.Configuration;
using Ray.IGrains.Actors;

namespace Ray.Grain
{
    public static class Configuration
    {
        public static void ConfigureFollowUnit(IServiceProvider serviceProvider, IFollowUnitContainer followUnitContainer)
        {
            followUnitContainer.Register(FollowUnitWithLong.From<Account>(serviceProvider).BindFlow<IAccountRep>().BindConcurrentFlow<IAccountFlow>().BindConcurrentFlow<IAccountDb>());
        }
        public static void Configure(this IServiceCollection serviceCollection)
        {
            serviceCollection.ConfigureArchive();
            serviceCollection.ConfigureBase();
        }
        public static void ConfigureArchive(this IServiceCollection serviceCollection)
        {
            serviceCollection.PostConfigure<ArchiveOptions<Account>>(options =>
            {
                options.On = true;
                options.EventClearOn = false;
                options.MaxIntervalMilliSeconds = 60 * 1000;
                options.IntervalVersion = 500;
            });
        }
        public static void ConfigureBase(this IServiceCollection serviceCollection)
        {
            serviceCollection.PostConfigure<CoreOptions<Account>>(options => options.ClearEventWhenOver = true);
        }
    }
}
