using ProtoBuf;
using Ray.Core.State;

namespace RayTest.IGrains.States
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public  class AsyncState<T> : IActorState<T>
    {
        public T StateId { get; set; }
        public long Version { get; set; }
        public long DoingVersion { get; set; }
    }
}
