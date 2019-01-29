namespace Ray.Core.Event
{
    public interface IEvent<K>
    {
        IEventBase<K> GetBase();
    }
}
