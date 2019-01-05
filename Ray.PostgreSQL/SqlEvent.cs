namespace Ray.Storage.PostgreSQL
{
    public class SqlEvent
    {
        public string TypeCode { get; set; }
        public byte[] Data { get; set; }
    }
}
