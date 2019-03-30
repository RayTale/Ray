namespace Ray.Core.Snapshot
{
    public class TxSnapshotBase<PrimaryKey> : SnapshotBase<PrimaryKey>
    {
        public TxSnapshotBase() { }
        public TxSnapshotBase(SnapshotBase<PrimaryKey> snapshotBase)
        {
            StateId = snapshotBase.StateId;
            DoingVersion = snapshotBase.DoingVersion;
            Version = snapshotBase.Version;
            StartTimestamp = snapshotBase.StartTimestamp;
            LatestMinEventTimestamp = snapshotBase.LatestMinEventTimestamp;
            IsLatest = snapshotBase.IsLatest;
            IsOver = snapshotBase.IsOver;
        }
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
        /// <summary>
        /// 清理事务相关信息
        /// </summary>
        public void ClearTransactionInfo(bool clearTransactionEvent)
        {
            if (TransactionStartVersion != -1)
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
        }
        public override SnapshotBase<PrimaryKey> Clone()
        {
            return new TxSnapshotBase<PrimaryKey>
            {
                StateId = StateId,
                DoingVersion = DoingVersion,
                Version = Version,
                StartTimestamp = StartTimestamp,
                LatestMinEventTimestamp = LatestMinEventTimestamp,
                IsLatest = IsLatest,
                IsOver = IsOver,
                TransactionId = TransactionId,
                TransactionStartVersion = TransactionStartVersion,
                TransactionStartTimestamp = TransactionStartTimestamp
            };
        }
    }
}
