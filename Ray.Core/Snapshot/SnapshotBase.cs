namespace Ray.Core.Snapshot
{
    public class SnapshotBase<PrimaryKey> : ISnapshotBase<PrimaryKey>, ICloneable<SnapshotBase<PrimaryKey>>
    {
        public PrimaryKey StateId { get; set; }
        public long DoingVersion { get; set; }
        public long Version { get; set; }
        public long StartTimestamp { get; set; }
        public long LatestMinEventTimestamp { get; set; }
        /// <summary>
        /// 当前正在进行中的事务Id(无需持久化)
        /// 本地事务不会记录该值
        /// </summary>
        public long TransactionId { get; set; }
        /// <summary>
        /// 事务开始时的版本号(无需持久化)
        /// 本地事务不会记录该值
        /// </summary>
        public long TransactionStartVersion { get; set; } = -1;
        /// <summary>
        /// 事务开始时的时间(无需持久化)
        /// 本地事务不会记录该值
        /// </summary>
        public long TransactionStartTimestamp { get; set; }
        public bool IsLatest { get; set; }
        public bool IsOver { get; set; }
        /// <summary>
        /// 清理事务相关信息
        /// </summary>
        public void ClearTransactionInfo(bool clearTransactionEvent)
        {
            TransactionStartVersion = -1;
            TransactionId = 0;
            TransactionStartTimestamp = 0;
            if (clearTransactionEvent)
            {
                Version -= 1;
                DoingVersion = Version;
            }
        }
        public SnapshotBase<PrimaryKey> Clone()
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
