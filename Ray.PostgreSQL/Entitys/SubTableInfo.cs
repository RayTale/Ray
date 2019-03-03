using Orleans.Concurrency;

namespace Ray.Storage.PostgreSQL.Entitys
{
    [Immutable]
    public class SubTableInfo
    {
        public string TableName { get; set; }
        public string SubTable { get; set; }
        public int Index { get; set; }
        public long StartTime { get; set; }
        public long EndTime { get; set; }
    }
}
