namespace Ray.Core.Abstractions.Monitor
{
    public class SummaryMetric
    {
        /// <summary>
        /// 活跃的Actor数量
        /// </summary>
        public int ActorLives { get; set; }
        /// <summary>
        /// 事件数量
        /// </summary>
        public int Events { get; set; }
        /// <summary>
        /// 异步产生的事件数量
        /// </summary>
        public int FollowEvents { get; set; }
        /// <summary>
        /// 忽略的时间量
        /// </summary>
        public int IgnoreEvents { get; set; }
        /// <summary>
        /// 时间戳
        /// </summary>
        public long Timestamp { get; set; }
    }
}
