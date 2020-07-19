namespace Ray.Core.Abstractions.Monitor
{
    public class SnapshotMetricElement
    {
        /// <summary>
        /// Actor的类型
        /// </summary>
        public string Actor { get; set; }

        /// <summary>
        /// 快照类型
        /// </summary>
        public string Snapshot { get; set; }

        /// <summary>
        /// 插入耗时
        /// </summary>
        public int SaveElapsedMs { get; set; }

        /// <summary>
        /// 间隔事件版本
        /// </summary>
        public int ElapsedVersion { get; set; }
    }
}
