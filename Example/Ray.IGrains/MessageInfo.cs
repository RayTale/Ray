using Orleans.Concurrency;
using ProtoBuf;
using Ray.Core.Abstractions;

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
