namespace Ray.Core.Event
{
    public interface IActorEvent<K> : IEvent, IActorOwned<K>
    {
    }
}
