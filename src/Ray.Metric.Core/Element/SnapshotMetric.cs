namespace Ray.Metric.Core.Element
{
    public class SnapshotMetric
    {
        /// <summary>
        /// Actor的类型
        /// </summary>
        public string Actor { get; set; }
        /// <summary>
        /// 快照类型
        /// </summary>
        public string Snapshot { get; set; }
        /// <summary>
        /// 保存次数
        /// </summary>
        public int SaveCount { get; set; }
        /// <summary>
        /// 插入最大耗时(ms)
        /// </summary>
        public int MaxSaveElapsedMs { get; set; }
        /// <summary>
        /// 插入最小耗时(ms)
        /// </summary>
        public int MinSaveElapsedMs { get; set; }
        /// <summary>
        /// 保存平均耗时(ms)
        /// </summary>
        public int AvgSaveElapsedMs { get; set; }
        /// <summary>
        /// 保存最大事件版本间隔
        /// </summary>
        public int MaxElapsedVersion { get; set; }
        /// <summary>
        /// 保存最小事件版本间隔
        /// </summary>
        public int MinElapsedVersion { get; set; }
        /// <summary>
        /// 保存平均事件版本间隔
        /// </summary>
        public int AvgElapsedVersion { get; set; }
        /// <summary>
        /// 统计时间戳
        /// </summary>
        public long Timestamp { get; set; }
    }
}
