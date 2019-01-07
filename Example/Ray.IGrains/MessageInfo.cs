using Orleans.Concurrency;
using ProtoBuf;
using Ray.Core.Serialization;

namespace Ray.IGrains
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    [Immutable]
    public class MessageInfo: IBytesWrapper
    {
        public string TypeName { get; set; }
        public byte[] Bytes { get; set; }
    }
}
