namespace Ray.Core.Abstractions.Monitor
{
    public class EventLinkMetric
    {
        /// <summary>
        /// 链路
        /// </summary>
        public EventLink Link { get; set; }
        /// <summary>
        /// 事件总数量
        /// </summary>
        public int Events { get; set; }
        /// <summary>
        /// 幂等忽略的事件量
        /// </summary>
        public int Ignores { get; set; }
        /// <summary>
        /// 过程最大耗时(ms)
        /// </summary>
        public int MaxElapsedMs { get; set; }
        /// <summary>
        /// 过程最小耗时(ms)
        /// </summary>
        public int MinElapsedMs { get; set; }
        /// <summary>
        /// 过程平均耗时(ms)
        /// </summary>
        public int AvgElapsedMs { get; set; }
        /// <summary>
        /// 统计时间戳
        /// </summary>
        public long Timestamp { get; set; }
    }
}
