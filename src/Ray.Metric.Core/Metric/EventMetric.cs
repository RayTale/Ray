namespace Ray.Metric.Core.Metric
{
    /// <summary>
    /// 事件监控指标
    /// </summary>
    public class EventMetric
    {
        /// <summary>
        /// 当前事件类型名称
        /// </summary>
        public string Event { get; set; }
        /// <summary>
        /// 事件来源的Actor名称
        /// </summary>
        public string Actor{ get; set; }
        /// <summary>
        /// 事件产生数量
        /// </summary>
        public int Events { get; set; }
        /// <summary>
        /// 单Grain最大数量
        /// </summary>
        public int MaxPerActor { get; set; }
        /// <summary>
        /// 单Grain最小数量
        /// </summary>
        public int MinPerActor { get; set; }
        /// <summary>
        /// 单Grain平均数量
        /// </summary>
        public int AvgPerActor { get; set; }
        /// <summary>
        /// 插入最大耗时(ms)
        /// </summary>
        public int MaxInsertElapsedMs { get; set; }
        /// <summary>
        /// 插入最小耗时(ms)
        /// </summary>
        public int MinInsertElapsedMs { get; set; }
        /// <summary>
        /// 插入平均耗时(ms)
        /// </summary>
        public int AvgInsertElapsedMs { get; set; }
        /// <summary>
        /// 因为幂等而过滤的事件量
        /// </summary>
        public int Ignores { get; set; }
        /// <summary>
        /// 统计时间戳
        /// </summary>
        public long Timestamp { get; set; }
    }
}
