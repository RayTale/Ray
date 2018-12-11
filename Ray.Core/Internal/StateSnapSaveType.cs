namespace Ray.Core.Internal
{
    /// <summary>
    /// Event Sourcing快照类型
    /// </summary>
    public enum StateSnapSaveType
    {
        /// <summary>
        /// 同步保存在当前Actor上
        /// </summary>
        Master,
        /// <summary>
        /// 快照在副本上保存(当前Actor不保存快照，但是激活的时候需要读取快照，)
        /// </summary>
        Replica
    }
}
