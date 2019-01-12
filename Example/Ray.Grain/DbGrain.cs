using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Ray.Core;
using Ray.Core.Event;
using Ray.Core.State;
using Ray.IGrains;

namespace Ray.Grain
{
    public abstract class DbGrain<K, E, S> : ConcurrentFollowGrain<K, E, S, MessageInfo>
        where E : IEventBase<K>
          where S : class, IActorState<K>, new()
    {
        public DbGrain(ILogger logger) : base(logger)
        {
        }
        protected override async ValueTask OnEventDelivered(IEvent<K, E> @event)
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
        protected abstract ValueTask Process(IEvent<K, E> @event);
    }
}
