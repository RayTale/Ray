namespace Ray.Core.Monitor
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
        /// 来源事件类型名称
        /// </summary>
        public string FromEvent { get; set; }
        /// <summary>
        /// 事件来源的Actor名称
        /// </summary>
        public string ActorName { get; set; }
        /// <summary>
        /// 事件产生数量
        /// </summary>
        public int Quantity { get; set; }
        /// <summary>
        /// 单Grain最大增长数量
        /// </summary>
        public int MaxIncreasePerActor { get; set; }
        /// <summary>
        /// 单Grain最小增长数量
        /// </summary>
        public int MinIncreasePerActor { get; set; }
        /// <summary>
        /// 单Grain平均增长数量
        /// </summary>
        public int AvgIncreasePerActor { get; set; }
        /// <summary>
        /// 与FromEvent的最大时间间隔(ms)
        /// </summary>
        public int MaxMoveIntervals { get; set; }
        /// <summary>
        /// 与FromEvent的最小时间间隔(ms)
        /// </summary>
        public int MinMoveIntervals { get; set; }
        /// <summary>
        /// 与FromEvent的平均时间间隔(ms)
        /// </summary>
        public int AvgMoveIntervals { get; set; }
        /// <summary>
        /// 插入最大耗时(ms)
        /// </summary>
        public int MaxInsertIntervals{ get; set; }
        /// <summary>
        /// 插入最小耗时(ms)
        /// </summary>
        public int MinInsertIntervals { get; set; }
        /// <summary>
        /// 插入平均耗时(ms)
        /// </summary>
        public int AvgInsertIntervals { get; set; }
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
