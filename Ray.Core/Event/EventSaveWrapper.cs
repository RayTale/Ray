namespace Ray.Core.Event
{
    public class EventSaveWrapper<K, E> where E : IEventBase<K>
    {
        public EventSaveWrapper(IEvent<K, E> evt, byte[] bytes, string uniqueId = null)
        {
            Event = evt;
            UniqueId = uniqueId;
            Bytes = bytes;
        }
        public IEvent<K, E> Event { get; set; }
        public string UniqueId { get; set; }
        public byte[] Bytes { get; set; }
        public bool ReturnValue { get; set; }
    }
}
