namespace Ray.Core.Configuration
{
    public class ArchiveOptions<GrainState>
    {
        /// <summary>
        /// 是否开启归档
        /// </summary>
        public bool On { get; set; } = true;
        /// <summary>
        /// 归档必须满足的间隔毫秒数
        /// </summary>
        public long IntervalMilliSeconds { get; set; } = 24 * 60 * 60 * 1000 * 7;
        /// <summary>
        /// 归档必须满足的间隔版本号
        /// </summary>
        public int IntervalVersion { get; set; } = 1000;
        /// <summary>
        /// 归档的最大间隔毫秒数，只要间隔大于该值则可以进行归档
        /// </summary>
        public long MaxIntervalMilliSeconds { get; set; } = (long)24 * 60 * 60 * 1000 * 30;
        /// <summary>
        /// 归档的最大版本号，只要间隔大于该值则可以进行归档
        /// </summary>
        public int MaxIntervalVersion { get; set; } = 10_000_000;
        /// <summary>
        /// 归档的最小版本间隔，用于Grain失活的时候判断是否做不完全归档
        /// </summary>
        public long MinIntervalVersion { get; set; } = 1;
        /// <summary>
        /// 是否开启事件清理
        /// </summary>
        public bool EventClearOn { get; set; } = true;
        /// <summary>
        /// 事件清理间隔的归档次数(防止清理过快导致幂等性失效)
        /// </summary>
        public int EventClearIntervalArchive { get; set; } = 3;
    }
}