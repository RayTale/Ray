namespace Ray.Core.Abstractions.Monitor
{
    public class EventMetricElement
    {
        /// <summary>
        /// 当前事件类型名称
        /// </summary>
        public string Event { get; set; }
        /// <summary>
        /// 来源事件类型名称
        /// </summary>
        public string FromEvent { get; set; }
        /// <summary>
        /// 事件归属Actor
        /// </summary>
        public string Actor { get; set; }
        /// <summary>
        /// 来源事件的Actor
        /// </summary>
        public string FromEventActor { get; set; }
        /// <summary>
        /// 事件来源的ActorId
        /// </summary>
        public string ActorId { get; set; }
        /// <summary>
        /// 插入耗时(ms)
        /// </summary>
        public int InsertElapsedMs { get; set; }
        /// <summary>
        /// 是否被忽略
        /// </summary>
        public bool Ignore { get; set; }
        /// <summary>
        /// 与前一个事件的时间间隔
        /// </summary>
        public int IntervalPrevious { get; set; }
    }
}
