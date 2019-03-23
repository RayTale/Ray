namespace Ray.Core.Event
{
    public class BatchAppendTransport<PrimaryKey>
    {
        public BatchAppendTransport(IFullyEvent<PrimaryKey> evt, in EventBytesTransport bytesTransport, string uniqueId = null)
        {
            Event = evt;
            UniqueId = uniqueId;
            BytesTransport = bytesTransport;
        }
        public IFullyEvent<PrimaryKey> Event { get; set; }
        public EventBytesTransport BytesTransport { get; set; }
        public string UniqueId { get; set; }
        public bool ReturnValue { get; set; }
    }
}
