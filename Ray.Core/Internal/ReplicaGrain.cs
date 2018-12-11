using System.Threading.Tasks;

namespace Ray.Core.Internal
{
    public abstract class ReplicaGrain<K, S, W> : FollowGrain<K, S, W>
        where S : class, IState<K>, new()
        where W : IMessageWrapper
    {
        protected override bool SaveSnapshot => false;
        protected override ValueTask OnEventDelivered(IEventBase<K> @event)
        {
            return Apply(State, @event);
        }
        protected abstract ValueTask Apply(S state, IEventBase<K> evt);
    }
}
