using ProtoBuf;
using Ray.Core.State;

namespace Ray.IGrains.States
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class StateBase<K> : ISnapshot<K>
    {
        public long DoingVersion { get; set; }
        public long Version { get; set; }
        public long LatestMinEventTimestamp { get; set; }
        public bool IsLatest { get; set; }
        public bool IsOver { get; set; }
        public K StateId { get; set; }
    }
}
