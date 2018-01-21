namespace Ray.PostgresqlES
{
    public class SqlEvent
    {
        public string Id { get; set; }
        public string StateId { get; set; }
        public string MsgId { get; set; }
        public string TypeCode { get; set; }
        public string Data { get; set; }
        public long Version { get; set; }
        public bool IsComplete { get; set; }
    }
}
