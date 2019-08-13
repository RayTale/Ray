using System.Collections.Generic;

namespace Ray.Storage.SQLServer
{
    public class SQLServerConnections
    {
        public Dictionary<string, string> ConnectionDict { get; set; } = new Dictionary<string, string>();
    }
}
