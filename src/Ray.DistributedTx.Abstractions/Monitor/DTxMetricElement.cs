namespace Ray.DistributedTx.Abstractions
{
    public class DTxMetricElement
    {
        /// <summary>
        /// 所属Actor
        /// </summary>
        public string Actor { get; set; }

        /// <summary>
        /// 执行耗时
        /// </summary>
        public int ElapsedMs { get; set; }

        /// <summary>
        /// 是否正常提交了
        /// </summary>
        public bool IsCommit { get; set; }

        /// <summary>
        /// 是否回滚了
        /// </summary>
        public bool IsRollback { get; set; }
    }
}
