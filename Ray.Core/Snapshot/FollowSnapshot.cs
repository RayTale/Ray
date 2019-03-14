namespace Ray.Core.Snapshot
{
    public class FollowSnapshot<PrimaryKey> : IFollowSnapshot<PrimaryKey>
    {
        public PrimaryKey StateId { get; set; }
        public long DoingVersion { get; set; }
        public long Version { get; set; }
        public long StartTimestamp { get; set; }
    }
}
