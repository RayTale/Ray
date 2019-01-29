using System;
using System.Collections.Generic;
using System.Text;
using Ray.Core;
using Ray.Core.Abstractions;
using Ray.IGrains.Actors;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.State;
using Ray.IGrains.States;
using Ray.Core.Configuration;

namespace Ray.Grain
{
    public static class Configuration
    {
        public static void ConfigureFollowUnit(IServiceProvider serviceProvider, IFollowUnitContainer followUnitContainer)
        {
            followUnitContainer.Register(FollowUnitWithLong.From<Account>(serviceProvider).BindFlow<IAccountRep>().BindConcurrentFlow<IAccountFlow>().BindConcurrentFlow<IAccountDb>());
        }
        public static void ConfigureArchive(this IServiceCollection serviceCollection)
        {
            serviceCollection.PostConfigure<ArchiveOptions<Account>>(options =>
            {
                options.On = false;
                options.IntervalVersion = 500;
            });
        }
        public static void ConfigureBase(this IServiceCollection serviceCollection)
        {
            serviceCollection.PostConfigure<CoreOptions<Account>>(options => options.ClearEventWhenOver = true);
        }
    }
}
