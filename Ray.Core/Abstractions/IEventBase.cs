namespace Ray.Core.Abstractions
{
    public interface IEventBase<K> : IEvent, IStateOwned<K>
    {
    }
}
