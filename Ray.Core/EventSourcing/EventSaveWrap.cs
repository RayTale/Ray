namespace Ray.Core.EventSourcing
{
    public class EventSaveWrap<K>
    {
        public EventSaveWrap(IEventBase<K> evt, byte[] bytes, string uniqueId = null, string hashKey = null)
        {
            Evt = evt;
            Bytes = bytes;
            UniqueId = uniqueId;
            HashKey = hashKey;
        }
        public IEventBase<K> Evt { get; set; }
        public byte[] Bytes { get; set; }
        public string UniqueId { get; set; }
        public string HashKey { get; set; }
    }
}
