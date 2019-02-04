namespace Ray.Core.State
{
    public class FollowSnapshot<K> : IFollowSnapshot<K>
    {
        public K StateId { get; set; }
        public long DoingVersion { get; set; }
        public long Version { get; set; }
        public long StartTimestamp { get; set; }
    }
}
