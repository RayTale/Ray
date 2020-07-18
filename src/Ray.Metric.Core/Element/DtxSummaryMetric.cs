namespace Ray.Metric.Core.Element
{
    public class DtxSummaryMetric
    {
        /// <summary>
        /// 事务执行次数
        /// </summary>
        public int Times { get; set; }

        /// <summary>
        /// 事务执行总次数
        /// </summary>
        public int Commits { get; set; }

        /// <summary>
        /// 回滚次数
        /// </summary>
        public int Rollbacks { get; set; }

        /// <summary>
        ///执行最大耗时(ms)
        /// </summary>
        public int MaxElapsedMs { get; set; }

        /// <summary>
        /// 执行最小耗时(ms)
        /// </summary>
        public int MinElapsedMs { get; set; }

        /// <summary>
        /// 执行平均耗时(ms)
        /// </summary>
        public int AvgElapsedMs { get; set; }

        /// <summary>
        /// 统计时间戳
        /// </summary>
        public long Timestamp { get; set; }
    }
}
