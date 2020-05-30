namespace Ray.Metrics.Metric
{
    public class EventLinkMetric
    {
        /// <summary>
        /// Actor的类型
        /// </summary>
        public string Actor { get; set; }
        /// <summary>
        /// 当前事件类型名称
        /// </summary>
        public string Event { get; set; }
        /// <summary>
        /// 上级Actor
        /// </summary>
        public string ParentActor { get; set; }
        /// <summary>
        /// 上级事件
        /// </summary>
        public string ParentEvent { get; set; }
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
