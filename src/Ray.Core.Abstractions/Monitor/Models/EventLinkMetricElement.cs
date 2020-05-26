namespace Ray.Core.Abstractions.Monitor
{
    public class EventLinkMetricElement
    {
        /// <summary>
        /// 当前事件类型
        /// </summary>
        public string Event { get; set; }
        /// <summary>
        /// 来源事件类型
        /// </summary>
        public string FromEvent { get; set; }
        /// <summary>
        /// Actor的类型
        /// </summary>
        public string Actor { get; set; }
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
