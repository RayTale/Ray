namespace Ray.Core.Configuration
{
    /// <summary>
    /// 归档配置
    /// </summary>
    public class ArchiveOptions
    {
        /// <summary>
        /// 是否开启归档
        /// </summary>
        public bool On { get; set; } = true;
        /// <summary>
        /// 归档必须满足的间隔秒数
        /// </summary>
        public long SecondsInterval { get; set; } = 24 * 60 * 60 * 7;
        /// <summary>
        /// 归档必须满足的间隔版本号
        /// </summary>
        public int VersionInterval { get; set; } = 1000;
        /// <summary>
        /// 归档的最大间隔秒数，只要间隔大于该值则可以进行归档(默认三十天)
        /// </summary>
        public long MaxSecondsInterval { get; set; } = 24 * 60 * 60 * 30;
        /// <summary>
        /// 归档的最大版本号，只要间隔大于该值则可以进行归档
        /// </summary>
        public int MaxVersionInterval { get; set; } = 10_000_000;
        /// <summary>
        /// 归档的最小版本间隔，用于Grain失活的时候判断是否做不完全归档
        /// </summary>
        public long MinVersionIntervalAtDeactivate { get; set; } = 1;
        /// <summary>
        /// 事件归档以来的快照归档次数(防止清理过快导致幂等性失效)
        /// </summary>
        public int MaxSnapshotArchiveRecords { get; set; } = 3;
        /// <summary>
        /// 是否开启事件清理
        /// tue:归档事件时会删除事件
        /// false:会把归档的事件移动到归档事件库
        /// </summary>
        public EventArchiveType EventArchiveType { get; set; } = EventArchiveType.Transfer;
    }
    /// <summary>
    /// 事件归档类型
    /// </summary>
    public enum EventArchiveType : byte
    {
        /// <summary>
        /// 转移到归档表
        /// </summary>
        Transfer = 0,
        /// <summary>
        /// 直接删除
        /// </summary>
        Delete = 1
    }
}