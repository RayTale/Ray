using Ray.Core.Event;

namespace RayTest.IGrains.Events
{
    public abstract class BaseEvent<K> : IEvent<K, EventBase<K>>
    {
        public abstract EventBase<K> Base { get; set; }
    }
}
