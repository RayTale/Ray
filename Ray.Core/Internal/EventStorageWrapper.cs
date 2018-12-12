namespace Ray.Core.Internal
{
    public class EventStorageWrapper<K>
    {
        public EventStorageWrapper(IEventBase<K> evt, string uniqueId = null, string hashKey = null)
        {
            Evt = evt;
            UniqueId = uniqueId;
            HashKey = hashKey;
        }
        public IEventBase<K> Evt { get; set; }
        public byte[] Bytes { get; set; }
        public string UniqueId { get; set; }
        public string HashKey { get; set; }
    }
}
