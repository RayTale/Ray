using System.Collections.Generic;

namespace Ray.Storage.PostgreSQL
{
    public class PSQLConnections
    {
        public Dictionary<string, string> ConnectionDict { get; set; } = new Dictionary<string, string>();
    }
}
