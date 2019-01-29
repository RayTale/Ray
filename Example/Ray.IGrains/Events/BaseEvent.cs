using Ray.Core.Event;

namespace Ray.IGrains.Events
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
