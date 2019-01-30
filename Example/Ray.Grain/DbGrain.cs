using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Ray.Core;
using Ray.Core.Event;

namespace Ray.Grain
{
    public abstract class DbGrain<Main, K> : ConcurrentFollowGrain<Main, K>
    {
        public DbGrain(ILogger logger) : base(logger)
        {
        }
        protected override async ValueTask OnEventDelivered(IEvent<K> @event)
        {
            var task = Process(@event);
            if (!task.IsCompletedSuccessfully)
            {
                await task.AsTask().ContinueWith(t =>
                {
                    if (t.Exception != null)
                    {
                        if (!(t.Exception.InnerException is Npgsql.PostgresException e && e.SqlState == "23505"))
                        {
                            ExceptionDispatchInfo.Capture(t.Exception).Throw();
                        }
                    }
                });
            }
        }
        protected abstract ValueTask Process(IEvent<K> @event);
    }
}
