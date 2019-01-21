using System;
using Ray.Core;
using Ray.Core.Abstractions;
using Ray.IGrains.Actors;
using Ray.IGrains.Events;

namespace Ray.Grain
{
    public class FollowUnitInit
    {
        public static void Register(IServiceProvider serviceProvider, IFollowUnitContainer followUnitContainer)
        {
            followUnitContainer.Register(FollowUnitWithLong<EventBase<long>>.From<Account>(serviceProvider).BindFlow<IAccountRep>().BindConcurrentFlow<IAccountFlow>().BindConcurrentFlow<IAccountDb>());
        }
    }
}
