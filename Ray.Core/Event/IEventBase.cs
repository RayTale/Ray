using Ray.Core.State;

namespace Ray.Core.Event
{
    public interface IEventBase<K> : IEvent, IStateOwned<K>
    {
    }
}
