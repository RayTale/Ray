namespace Ray.Storage.PostgreSQL
{
    public class EventBytesWrapper
    {
        public string StateId { get; set; }
        public string TypeCode { get; set; }
        public byte[] Data { get; set; }
        public long Version { get; set; }
        public long Timestamp { get; set; }
    }
}
