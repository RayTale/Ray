namespace Ray.Storage.SQLCore
{
    /// <summary>
    /// 分表信息
    /// </summary>
    public class EventSubTable
    {
        /// <summary>
        /// 表名
        /// </summary>
        public string TableName { get; set; }
        /// <summary>
        /// 子表名称
        /// </summary>
        public string SubTable { get; set; }
        /// <summary>
        /// 分表顺序
        /// </summary>
        public int Index { get; set; }
        /// <summary>
        /// 分表的开始时间
        /// </summary>
        public long StartTime { get; set; }
        /// <summary>
        /// 分表的结束时间
        /// </summary>
        public long EndTime { get; set; }
    }
}
