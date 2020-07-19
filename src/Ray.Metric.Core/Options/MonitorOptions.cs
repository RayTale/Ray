namespace Ray.Metric.Core.Options
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
        public int ActorMetricFrequency { get; set; } = 5;

        /// <summary>
        /// 事件链路指标统计频率(s)
        /// </summary>
        public int EventLinkMetricFrequency { get; set; } = 5;

        /// <summary>
        /// 异步指标统计频率(s)
        /// </summary>
        public int FollowActorMetricFrequency { get; set; } = 5;

        /// <summary>
        /// 异步事件指标统计频率(s)
        /// </summary>
        public int FollowEventMetricFrequency { get; set; } = 5;

        /// <summary>
        /// 异步事件分组指标统计频率(s)
        /// </summary>
        public int FollowGroupMetricFrequency { get; set; } = 5;

        /// <summary>
        /// 快照指标统计频率(s)
        /// </summary>
        public int SnapshotMetricFrequency { get; set; } = 5;

        /// <summary>
        /// 分布式事务指标统计频率(s)
        /// </summary>
        public int DtxMetricFrequency { get; set; } = 5;
    }
}
