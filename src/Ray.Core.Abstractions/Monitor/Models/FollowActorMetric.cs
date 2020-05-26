namespace Ray.Core.Abstractions.Monitor
{
    public class FollowActorMetric
    {
        /// <summary>
        /// Actor的类型
        /// </summary>
        public string Actor { get; set; }
        /// <summary>
        /// 归属的Actor的类型
        /// </summary>
        public string FromActor { get; set; }
        /// <summary>
        /// 执行的事件量
        /// </summary>
        public int Events{ get; set; }
        /// <summary>
        /// 最大执行时间间隔(ms)
        /// </summary>
        public int MaxElapsedMs { get; set; }
        /// <summary>
        /// 最小执行时间间隔(ms)
        /// </summary>
        public int MinElapsedMs { get; set; }
        /// <summary>
        /// 平均执行时间间隔(ms)
        /// </summary>
        public int AvgElapsedMs { get; set; }
        /// <summary>
        /// 统计时间戳
        /// </summary>
        public long Timestamp { get; set; }
    }
}
