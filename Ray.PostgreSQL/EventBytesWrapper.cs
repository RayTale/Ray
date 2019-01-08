namespace Ray.Storage.PostgreSQL
{
    public class EventBytesWrapper
    {
        public string TypeCode { get; set; }
        public byte[] Data { get; set; }
    }
}
