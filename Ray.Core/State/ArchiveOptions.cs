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
        public int MaxMilliSeconds { get; set; }
        /// <summary>
        /// 归档的最大版本号，只要间隔大于该值则可以进行归档
        /// </summary>
        public long MaxIntervaleVersion { get; set; }
    }
}