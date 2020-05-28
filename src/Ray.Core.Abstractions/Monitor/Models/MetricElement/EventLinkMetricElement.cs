namespace Ray.Core.Abstractions.Monitor
{
    public class EventLinkMetricElement
    {
        /// <summary>
        /// Actor的类型
        /// </summary>
        public string Actor { get; set; }
        /// <summary>
        /// 来源的Actor类型
        /// </summary>
        public string FromEventActor { get; set; }
        /// <summary>
        /// 当前事件类型
        /// </summary>
        public string Event { get; set; }
        /// <summary>
        /// 来源事件类型
        /// </summary>
        public string FromEvent { get; set; }
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
    }
}
