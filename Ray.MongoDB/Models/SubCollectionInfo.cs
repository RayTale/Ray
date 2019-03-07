using MongoDB.Bson.Serialization.Attributes;

namespace Ray.Storage.Mongo
{
    public class SubCollectionInfo
    {
        [BsonId]
        public string Id { get; set; }
        public string Table { get; set; }
        public string SubTable { get; set; }
        public int Index { get; set; }
        public long StartTime { get; set; }
        public long EndTime { get; set; }
    }
}
