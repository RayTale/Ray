using ProtoBuf;
using Ray.Core.Event;

namespace Ray.IGrains.Events
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class EventBase<K> : IEventBase<K>
    {
        public K StateId { get; set; }
        public long Version { get; set; }
        public long Timestamp { get; set; }
    }
}
