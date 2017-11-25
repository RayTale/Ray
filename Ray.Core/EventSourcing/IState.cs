using System;

namespace Ray.Core.EventSourcing
{
    public interface IState<K>
    {
        K StateId { get; set; }
        /// <summary>
        /// 状态的版本号
        /// </summary>
        UInt32 Version { get; set; }
        /// <summary>
        /// 状态版本号对应的Event时间
        /// </summary>
        DateTime VersionTime { get; set; }
    }
}
