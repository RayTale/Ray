using Microsoft.Extensions.Logging;
using Ray.Core;
using Ray.Core.Event;
using System;
using System.Threading.Tasks;

namespace Ray.Grain
{
    public abstract class DbGrain<Main, K> : ObserverGrain<Main, K>
    {
        protected override async ValueTask EventDelivered(IFullyEvent<K> @event)
        {
            var task = base.EventDelivered(@event);
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
                        throw;
                    }
                }
            }
        }
    }
}
