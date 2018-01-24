using ProtoBuf;
using Ray.Core.EventSourcing;
using System;

namespace Ray.PostgresqlES
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllPublic)]
    public class ToDbState<K> : IState<K>
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
        public uint DoingVersion { get; set; }
    }
}
