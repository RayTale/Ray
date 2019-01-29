using Ray.Core.Event;

namespace RayTest.IGrains.Events
{
    public abstract class BaseEvent<K> : IEvent<K>
    {
        public abstract EventBase<K> Base { get; set; }
        public IEventBase<K> GetBase()
        {
            return Base;
        }
    }
}
