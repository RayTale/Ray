namespace Ray.PostgresqlES
{
    public class SqlEvent
    {
        public string TypeCode { get; set; }
        public string Data { get; set; }
        public bool IsComplete { get; set; }
    }
}
