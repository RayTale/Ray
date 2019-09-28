namespace Ray.Core.Event
{
    public class EventTransport<PrimaryKey>
    {
        public EventTransport(FullyEvent<PrimaryKey> fullyEvent, string uniqueId, string hashKey)
        {
            FullyEvent = fullyEvent;
            UniqueId = uniqueId;
            HashKey = hashKey;
        }
        public FullyEvent<PrimaryKey> FullyEvent { get; set; }
        public EventBytesTransport BytesTransport { get; set; }
        public string UniqueId { get; set; }
        public string HashKey { get; set; }
    }
}
