using Ray.Core.Abstractions;

namespace Ray.Core.Snapshot
{
    public interface IObserverSnapshot<PrimaryKey> : IActorOwned<PrimaryKey>
    {
        /// <summary>
        /// 正在处理中的Version
        /// </summary>
        long DoingVersion { get; set; }
        /// <summary>
        /// 状态的版本号
        /// </summary>
        long Version { get; set; }
        /// <summary>
        /// 状态的开始时间
        /// </summary>
        long StartTimestamp { get; set; }
    }
}
