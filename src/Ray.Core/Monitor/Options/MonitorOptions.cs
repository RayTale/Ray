namespace Ray.Core.Monitor
{
    public class MonitorOptions
    {
        /// <summary>
        /// 事件指标统计频率(s)
        /// </summary>
        public int EventMetricFrequency { get; set; } = 5;
        /// <summary>
        /// Actor指标统计频率(s)
        /// </summary>
        public int ActorMetricFrequency { get; set; }
        /// <summary>
        /// 事件链路指标统计频率(s)
        /// </summary>
        public int EventLinkMetricFrequency { get; set; }
        /// <summary>
        /// 异步指标统计频率(s)
        /// </summary>
        public int FollowActorMetricFrequency { get; set; }
        /// <summary>
        /// 异步事件指标统计频率(s)
        /// </summary>
        public int FollowEventMetricFrequency { get; set; }
    }
}
