namespace Ray.Core.State
{
    public class ArchiveOptions
    {
        /// <summary>
        /// 是否开启归档
        /// </summary>
        public bool On { get; set; }
        /// <summary>
        /// 归档必须满足的间隔毫秒数
        /// </summary>
        public int IntervalMilliseconds { get; set; }
        /// <summary>
        /// 归档必须满足的间隔版本号
        /// </summary>
        public long IntervalVersion { get; set; }
        /// <summary>
        /// 归档的最大间隔毫秒数，只要间隔大于该值则可以进行归档
        /// </summary>
        public int MaxIntervalMilliSeconds { get; set; }
        /// <summary>
        /// 归档的最大版本号，只要间隔大于该值则可以进行归档
        /// </summary>
        public long MaxIntervalVersion { get; set; }
        /// <summary>
        /// 归档的最小版本间隔，用于Grain失活的时候判断是否做不完全归档
        /// </summary>
        public long MinIntervalVersion { get; set; }
        /// <summary>
        /// 事件清理配置信息
        /// </summary>
        public ArchiveEventClearOptions EventClearOptions { get; set; }
    }
}