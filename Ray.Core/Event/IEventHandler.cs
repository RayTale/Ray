using Ray.Core.State;

namespace Ray.Core.Event
{
    public interface IEventHandler<K, E, S>
        where E : IEventBase<K>
        where S : class, IActorState<K>, new()
    {
        void Apply(S state, IEvent<K, E> evt);
    }
}
