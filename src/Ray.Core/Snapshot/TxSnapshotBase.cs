namespace Ray.Core.Snapshot
{
    public class TxSnapshotBase<PrimaryKey> : SnapshotBase<PrimaryKey>
    {
        public TxSnapshotBase()
        {
        }

        public TxSnapshotBase(SnapshotBase<PrimaryKey> snapshotBase)
        {
            this.StateId = snapshotBase.StateId;
            this.DoingVersion = snapshotBase.DoingVersion;
            this.Version = snapshotBase.Version;
            this.StartTimestamp = snapshotBase.StartTimestamp;
            this.LatestMinEventTimestamp = snapshotBase.LatestMinEventTimestamp;
            this.IsLatest = snapshotBase.IsLatest;
            this.IsOver = snapshotBase.IsOver;
            this.TransactionId = string.Empty;
        }

        /// <summary>
        /// 当前正在进行中的事务Id(无需持久化)
        /// 本地事务不会记录该值
        /// </summary>
        public string TransactionId { get; set; }

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
            if (this.TransactionStartVersion != -1)
            {
                this.TransactionStartVersion = -1;
                this.TransactionId = string.Empty;
                this.TransactionStartTimestamp = 0;
                if (clearTransactionEvent)
                {
                    this.Version -= 1;
                    this.DoingVersion = this.Version;
                }
            }
        }

        public override SnapshotBase<PrimaryKey> Clone()
        {
            return new TxSnapshotBase<PrimaryKey>
            {
                StateId = this.StateId,
                DoingVersion = this.DoingVersion,
                Version = this.Version,
                StartTimestamp = this.StartTimestamp,
                LatestMinEventTimestamp = this.LatestMinEventTimestamp,
                IsLatest = this.IsLatest,
                IsOver = this.IsOver,
                TransactionId = this.TransactionId,
                TransactionStartVersion = this.TransactionStartVersion,
                TransactionStartTimestamp = this.TransactionStartTimestamp
            };
        }
    }
}
