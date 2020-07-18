namespace Ray.Core
{
    /// <summary>
    /// Over类别
    /// </summary>
    public enum OverType : byte
    {
        /// <summary>
        /// 不进行任何操作
        /// </summary>
        None = 0,
        /// <summary>
        /// 归档事件(采用当前Grain的归档配置)
        /// </summary>
        ArchivingEvent = 0,
        /// <summary>
        /// 删除事件
        /// </summary>
        DeleteEvent = 1,
        /// <summary>
        /// 删除事件、快照、归档数据
        /// </summary>
        DeleteAll = 2
    }
}
