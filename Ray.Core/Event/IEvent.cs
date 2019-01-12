namespace Ray.Core.Event
{
    public interface IEvent<K, E> where E : IEventBase<K>
    {
        E Base { get; set; }
    }
}
