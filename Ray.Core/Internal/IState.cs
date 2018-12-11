using System;

namespace Ray.Core.Internal
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
        /// <summary>
        /// 状态版本号对应的Event时间
        /// </summary>
        DateTime VersionTime { get; set; }
    }
}
