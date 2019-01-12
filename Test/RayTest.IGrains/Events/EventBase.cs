using ProtoBuf;
using Ray.Core.Event;

namespace RayTest.IGrains.Events
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class EventBase<K> : IEventBase<K>
    {
        public long Version { get; set; }
        public long Timestamp { get; set; }
        public K StateId { get; set; }
    }
}
