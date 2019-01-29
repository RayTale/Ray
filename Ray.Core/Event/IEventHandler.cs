using Ray.Core.State;

namespace Ray.Core.Event
{
    public interface IEventHandler<K, S>
        where S : class, new()
    {
        void Apply(Snapshot<K, S> state, IEvent<K> evt);
    }
}
