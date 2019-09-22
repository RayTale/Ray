using Ray.Core.Event;
using Ray.DistributedTx;
using System;
using System.Threading.Tasks;

namespace Ray.Grain
{
    public abstract class DbGrain<PrimaryKey, Main> : DTxObserverGrain<PrimaryKey, Main>
    {
        protected override async ValueTask EventDelivered(IFullyEvent<PrimaryKey> @event)
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
