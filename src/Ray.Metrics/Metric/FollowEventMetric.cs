namespace Ray.Metrics.Metric
{
    public class FollowEventMetric
    {
        /// <summary>
        /// Actor的类型
        /// </summary>
        public string Actor { get; set; }
        /// <summary>
        /// 归属的Actor的类型
        /// </summary>
        public string FromActor { get; set; }
        /// <summary>
        /// 事件
        /// </summary>
        public string Event { get; set; }
        /// <summary>
        /// 事件量
        /// </summary>
        public int Events { get; set; }
        /// <summary>
        /// 事件送达的最大耗时(ms)
        /// </summary>
        public int MaxDeliveryElapsedMs { get; set; }
        /// <summary>
        /// 事件送达的平均耗时(ms)
        /// </summary>
        public int AvgDeliveryElapsedMs { get; set; }
        /// <summary>
        /// 事件送达的最小耗时(ms)
        /// </summary>
        public int MinDeliveryElapsedMs { get; set; }
        /// <summary>
        /// 最大执行时间间隔(ms)
        /// </summary>
        public int MaxElapsedMs { get; set; }
        /// <summary>
        /// 最小执行时间间隔(ms)
        /// </summary>
        public int MinElapsedMs { get; set; }
        /// <summary>
        /// 平均执行时间间隔(ms)
        /// </summary>
        public int AvgElapsedMs { get; set; }
        /// <summary>
        /// 统计时间戳
        /// </summary>
        public long Timestamp { get; set; }
    }
}
