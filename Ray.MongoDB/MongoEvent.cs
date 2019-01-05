using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Ray.Storage.MongoDB
{
    public class MongoEvent<K>
    {
        [BsonId]
        public ObjectId Id { get; set; }
        public K StateId
        {
            get;
            set;
        }
        public string UniqueId { get; set; }
        public Int64 Version
        {
            get;
            set;
        }

        public string TypeCode
        {
            get;
            set;
        }

        public byte[] Data
        {
            get;
            set;
        }
    }
}
