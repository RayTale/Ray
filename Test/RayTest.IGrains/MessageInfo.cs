using Orleans.Concurrency;
using ProtoBuf;
using Ray.Core.Messaging;

namespace RayTest.IGrains
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    [Immutable]
    public class MessageInfo: IBytesMessage
    {
        public string TypeName { get; set; }
        public byte[] Bytes { get; set; }
    }
}
