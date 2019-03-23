namespace Ray.Core.Snapshot
{
    public interface ISnapshotBase<PrimaryKey> : IActorOwned<PrimaryKey>
    {
        /// <summary>
        /// 正在处理中的Version
        /// </summary>
        long DoingVersion { get; set; }
        /// <summary>
        /// 状态的版本号
        /// </summary>
        long Version { get; set; }
        /// <summary>
        /// 第一个事件的时间戳
        /// </summary>
        long StartTimestamp { get; set; }
        /// <summary>
        /// 最新事件的最小时间戳(方便读取后续事件列表)
        /// </summary>
        long LatestMinEventTimestamp { get; set; }
        /// <summary>
        /// 是否是最新状态(如果是最新状态则不需要从事件库恢复)
        /// </summary>
        bool IsLatest { get; set; }
        /// <summary>
        /// 状态已经终结，不允许再产生新的事件
        /// </summary>
        bool IsOver { get; set; }
    }
}
