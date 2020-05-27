namespace Ray.Core.Abstractions.Monitor
{
    public class SummaryMetric
    {
        /// <summary>
        /// 活跃的Actor数量
        /// </summary>
        public int ActorLives { get; set; }
        /// <summary>
        /// 事件总数量
        /// </summary>
        public int Events { get; set; }
        /// <summary>
        /// 幂等忽略的事件量
        /// </summary>
        public int Ignores { get; set; }
        /// <summary>
        /// 单Actor最大事件量
        /// </summary>
        public int MaxEventsPerActor { get; set; }
        /// <summary>
        /// 单Actor平均事件量
        /// </summary>
        public int AvgEventsPerActor { get; set; }
        /// <summary>
        /// 单Actor最小事件量
        /// </summary>
        public int MinEventsPerActor { get; set; }
        /// <summary>
        /// 时间戳
        /// </summary>
        public long Timestamp { get; set; }
    }
}
