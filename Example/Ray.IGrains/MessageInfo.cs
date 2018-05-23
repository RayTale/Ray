using Orleans.Concurrency;
using ProtoBuf;
using Ray.Core;

namespace Ray.IGrains
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    [Immutable]
    public class MessageInfo: IMessageWrapper
    {
        public string TypeCode { get; set; }
        public byte[] BinaryBytes { get; set; }
    }
}
