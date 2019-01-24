namespace Ray.Core.State
{
    public class ArchiveEventClearOptions
    {
        /// <summary>
        /// 是否开启事件清理
        /// </summary>
        public bool On { get; set; }
        /// <summary>
        /// 事件清理间隔的归档次数(防止清理过快导致幂等性失效)
        /// </summary>
        public int IntervalArchive { get; set; }
    }
}
