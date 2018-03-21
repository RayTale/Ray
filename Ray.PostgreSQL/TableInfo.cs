using System;

namespace Ray.PostgreSQL
{
    public class TableInfo
    {
        public string Prefix { get; set; }
        public string Name { get; set; }
        public int Version { get; set; }
        public DateTime CreateTime { get; set; }
    }
}
