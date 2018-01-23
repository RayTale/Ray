namespace Ray.PostgresqlES
{
    public class SqlEvent
    {
        public string TypeCode { get; set; }
        public byte[] Data { get; set; }
        public bool IsComplete { get; set; }
    }
}
