using System.Collections.Generic;

namespace Ray.Storage.MongoDB
{
    public class MongoConnections
    {
        public Dictionary<string, string> ConnectionDict { get; set; } = new Dictionary<string, string>();
    }
}
