namespace Ray.Core.Event
{
    public class EventTransmitWrapper<K, E>
        where E : IEventBase<K>
    {
        public EventTransmitWrapper(IEvent<K, E> evt, string uniqueId = null, string hashKey = null)
        {
            Evt = evt;
            UniqueId = uniqueId;
            HashKey = hashKey;
        }
        public IEvent<K, E> Evt { get; set; }
        public byte[] Bytes { get; set; }
        public string UniqueId { get; set; }
        public string HashKey { get; set; }
    }
}
