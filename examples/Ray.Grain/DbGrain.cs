using System;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Ray.Core;
using Ray.Core.Event;

namespace Ray.Grain
{
    public abstract class DbGrain<Main, K> : ConcurrentObserverGrain<Main, K>
    {
        public DbGrain(ILogger logger) : base(logger)
        {
        }
        protected override async ValueTask OnEventDelivered(IFullyEvent<K> @event)
        {
            var task = Process(@event);
            if (!task.IsCompletedSuccessfully)
            {
                try
                {
                    await task;
                }
                catch (Exception ex)
                {
                    if (!(ex is Npgsql.PostgresException e && e.SqlState == "23505"))
                    {
                        ExceptionDispatchInfo.Capture(ex).Throw();
                    }
                }
            }
        }
        protected abstract ValueTask Process(IFullyEvent<K> @event);
    }
}
