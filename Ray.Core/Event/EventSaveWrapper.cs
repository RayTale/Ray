namespace Ray.Core.Event
{
    public class EventSaveWrapper<K>
    {
        public EventSaveWrapper(IEvent<K> evt, byte[] bytes, string uniqueId = null)
        {
            Event = evt;
            UniqueId = uniqueId;
            Bytes = bytes;
        }
        public IEvent<K> Event { get; set; }
        public string UniqueId { get; set; }
        public byte[] Bytes { get; set; }
        public bool ReturnValue { get; set; }
    }
}
