namespace Ray.Metric.Core.Element
{
    public class ActorMetric
    {
        /// <summary>
        /// Actor的类型
        /// </summary>
        public string Actor { get; set; }

        /// <summary>
        /// 存活的Actor数量
        /// </summary>
        public int Lives { get; set; }

        /// <summary>
        /// 总的事件量
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
        /// 统计时间戳
        /// </summary>
        public long Timestamp { get; set; }
    }
}
