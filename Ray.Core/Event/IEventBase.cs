namespace Ray.Core.Event
{
    public interface IEventBase<K> : IActorOwned<K>
    {
        long Version { get; set; }
        long Timestamp { get; set; }
    }
}
