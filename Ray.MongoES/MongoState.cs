using MongoDB.Bson.Serialization.Attributes;

namespace Ray.MongoDb
{
    public class MongoState<K>
    {
        [BsonId]
        public string Id { get; set; }
        public K StateId { get; set; }
        public byte[] Data { get; set; }
    }
}
