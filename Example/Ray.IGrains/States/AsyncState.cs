using ProtoBuf;
using Ray.Core.Internal;
using System;

namespace Ray.IGrains.States
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public  class AsyncState<T> : IState<T>
    {
        public T StateId { get; set; }
        public Int64 Version { get; set; }
        public Int64 DoingVersion { get; set; }
        public DateTime VersionTime { get; set; }
    }
}
