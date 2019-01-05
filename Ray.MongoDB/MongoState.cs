using MongoDB.Bson.Serialization.Attributes;

namespace Ray.Storage.MongoDB
{
    public class MongoState<K>
    {
        [BsonId]
        public string Id { get; set; }
        public K StateId { get; set; }
        public byte[] Data { get; set; }
        public long Version { get; set; }
    }
}
