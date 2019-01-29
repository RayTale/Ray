namespace Ray.Core.State
{
    public class SnapshotBase<K> : ISnapshot<K>, ICloneable<SnapshotBase<K>>
    {
        public K StateId { get; set; }
        public long DoingVersion { get; set; }
        public long Version { get; set; }
        public long LatestMinEventTimestamp { get; set; }
        public bool IsLatest { get; set; }
        public bool IsOver { get; set; }

        public SnapshotBase<K> Clone()
        {
            return new SnapshotBase<K>
            {
                StateId = StateId,
                DoingVersion = DoingVersion,
                Version = Version,
                LatestMinEventTimestamp = LatestMinEventTimestamp,
                IsLatest = IsLatest,
                IsOver = IsOver
            };
        }
    }
}
