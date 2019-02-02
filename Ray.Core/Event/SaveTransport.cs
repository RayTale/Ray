namespace Ray.Core.Event
{
    public class SaveTransport<PrimaryKey>
    {
        public SaveTransport(FullyEvent<PrimaryKey> evt, BytesTransport bytesTransport, string uniqueId = null)
        {
            Event = evt;
            UniqueId = uniqueId;
            BytesTransport = bytesTransport;
        }
        public FullyEvent<PrimaryKey> Event { get; set; }
        public BytesTransport BytesTransport { get; set; }
        public string UniqueId { get; set; }
        public bool ReturnValue { get; set; }
    }
}
