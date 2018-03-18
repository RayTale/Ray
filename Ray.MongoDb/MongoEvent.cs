using System;
using MongoDB.Bson.Serialization.Attributes;

namespace Ray.MongoDb
{
    public class MongoEvent<K>
    {
        [BsonId]
        public string Id { get; set; }
        public K StateId
        {
            get;
            set;
        }
        public string MsgId { get; set; }
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
