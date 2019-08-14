using System.Collections.Generic;

namespace Ray.Storage.Mongo
{
    public class MongoConnections
    {
        public Dictionary<string, string> ConnectionDict { get; set; } = new Dictionary<string, string>();
    }
}
