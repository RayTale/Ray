namespace Ray.Core.State
{
    public interface IActorState<K> : IActorOwned<K>
    {
        /// <summary>
        /// 正在处理中的Version
        /// </summary>
        long DoingVersion { get; set; }
        //long StartVersion { get; set; }
        /// <summary>
        /// 状态的版本号
        /// </summary>
        long Version { get; set; }
        //long MinTimestamp { get; set; }
        //long MaxTimestamp { get; set; }
        ///// <summary>
        ///// 下一个事件的时间戳(方便读取后续事件列表)
        ///// </summary>
        //long NextTimestamp { get; set; }
        //bool IsLatest { get; set; }
        ///// <summary>
        ///// 是否是归档状态
        ///// </summary>
        //bool IsArchive { get; set; }
        ///// <summary>
        ///// 状态已经终结，不允许再产生新的事件
        ///// </summary>
        //bool IsOver { get; set; }
    }
}
