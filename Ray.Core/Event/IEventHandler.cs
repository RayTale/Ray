using Ray.Core.State;

namespace Ray.Core.Event
{
    public interface IEventHandler<K, E, S, B>
        where E : IEventBase<K>
        where B : ISnapshot<K>, new()
        where S : class, IState<K, B>, new()
    {
        void Apply(S state, IEvent<K, E> evt);
    }
}
