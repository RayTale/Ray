namespace Ray.Core.Event
{
    public class EventTransmitWrapper<K>
    {
        public EventTransmitWrapper(IActorEvent<K> evt, string uniqueId = null, string hashKey = null)
        {
            Evt = evt;
            UniqueId = uniqueId;
            HashKey = hashKey;
        }
        public IActorEvent<K> Evt { get; set; }
        public byte[] Bytes { get; set; }
        public string UniqueId { get; set; }
        public string HashKey { get; set; }
    }
}
