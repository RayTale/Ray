using ProtoBuf;

namespace Ray.IGrains.States
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class FollowState<K> : BaseState<K>
    {
        public override StateBase<K> Base { get; set; }
    }
}
