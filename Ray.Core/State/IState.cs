namespace Ray.Core.State
{
    public interface IState<K>
    {
        K StateId { get; set; }
        /// <summary>
        /// 状态的版本号
        /// </summary>
        long Version { get; set; }
        /// <summary>
        /// 正在处理中的Version
        /// </summary>
        long DoingVersion { get; set; }
    }
}
