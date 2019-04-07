using System.Collections.Generic;

namespace Ray.Storage.MySQL
{
    public class MySQLConnections
    {
        public Dictionary<string, string> ConnectionDict { get; set; } = new Dictionary<string, string>();
    }
}
