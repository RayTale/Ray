using Ray.Core.Serialization;

namespace Ray.Core.Event
{
    public class SaveTransport<PrimaryKey>
    {
        public SaveTransport(FullyEvent<PrimaryKey> evt, EventBytesTransport bytesTransport, string uniqueId = null)
        {
            Event = evt;
            UniqueId = uniqueId;
            BytesTransport = bytesTransport;
        }
        public FullyEvent<PrimaryKey> Event { get; set; }
        public EventBytesTransport BytesTransport { get; set; }
        public string UniqueId { get; set; }
        public bool ReturnValue { get; set; }
    }
}
