using ProtoBuf;
using Ray.Core.EventSourcing;
using System;

namespace Ray.MongoES
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllPublic)]
    public class ToReadState<K> : IState<K>
    {
        public K StateId
        {
            get;
            set;
        }

        public UInt32 Version
        {
            get;
            set;
        }
        public DateTime VersionTime
        {
            get;
            set;
        }
    }
}
