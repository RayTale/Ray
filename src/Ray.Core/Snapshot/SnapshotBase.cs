namespace Ray.Core.Snapshot
{
    public class SnapshotBase<PrimaryKey> : ICloneable<SnapshotBase<PrimaryKey>>
    {
        public PrimaryKey StateId { get; set; }
        public long DoingVersion { get; set; }
        public long Version { get; set; }
        public long StartTimestamp { get; set; }
        public long LatestMinEventTimestamp { get; set; }
        public bool IsLatest { get; set; }
        public bool IsOver { get; set; }
        public virtual SnapshotBase<PrimaryKey> Clone()
        {
            return new SnapshotBase<PrimaryKey>
            {
                StateId = StateId,
                DoingVersion = DoingVersion,
                Version = Version,
                StartTimestamp = StartTimestamp,
                LatestMinEventTimestamp = LatestMinEventTimestamp,
                IsLatest = IsLatest,
                IsOver = IsOver
            };
        }
    }
}
