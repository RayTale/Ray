using Ray.Core.State;

namespace Ray.Core.Event
{
    public interface IEventHandler<K, E, S, B>
        where E : IEventBase<K>
        where B : IStateBase<K>, new()
        where S : class, IState<K, B>, new()
    {
        void Apply(S state, IEvent<K, E> evt);
    }
}
