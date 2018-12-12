using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Ray.Core.Internal;
using Ray.IGrains;

namespace Ray.Grain
{
    public abstract class DbGrain<K, S> : FollowGrain<K, S, MessageInfo>
          where S : class, IState<K>, new()
    {
        public DbGrain(ILogger logger) : base(logger)
        {
        }
        protected override async ValueTask OnEventDelivered(IEventBase<K> @event)
        {
            var task = Process(@event);
            if (!task.IsCompleted)
            {
                await task.AsTask().ContinueWith(t =>
                {
                    if (t.Exception != null)
                    {
                        if (!(t.Exception.InnerException is Npgsql.PostgresException e && e.SqlState == "23505"))
                        {
                            throw t.Exception;
                        }
                    }
                });
            }
        }
        protected abstract ValueTask Process(IEventBase<K> @event);
    }
}
