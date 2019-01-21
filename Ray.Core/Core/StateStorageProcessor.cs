namespace Ray.Core
{
    /// <summary>
    /// 状态快照保存方式
    /// </summary>
    public enum StateStorageProcessor
    {
        /// <summary>
        /// 同步保存在当前Actor上
        /// </summary>
        Master,
        /// <summary>
        /// 快照在副本上保存(当前Actor不保存快照，但是激活的时候需要读取快照)
        /// </summary>
        Replica
    }
}
