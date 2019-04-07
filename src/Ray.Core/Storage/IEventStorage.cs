using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.Event;

namespace Ray.Core.Storage
{
    public interface IEventStorage<PrimaryKey>
    {
        /// <summary>
        /// 单事件插入
        /// </summary>
        /// <param name="fullyEvent"></param>
        /// <param name="bytesTransport"></param>
        /// <param name="unique"></param>
        /// <returns></returns>
        Task<bool> Append(IFullyEvent<PrimaryKey> fullyEvent, in EventBytesTransport bytesTransport, string unique);
        /// <summary>
        /// 批量事件插入
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        Task TransactionBatchAppend(List<EventTransport<PrimaryKey>> list);
        /// <summary>
        /// 批量获取事件
        /// </summary>
        /// <param name="stateId">状态Id，相当于GrainId</param>
        /// <param name="latestTimestamp">将要拉取列表的起点时间</param>
        /// <param name="startVersion">开始版本</param>
        /// <param name="endVersion">结束版本</param>
        /// <returns></returns>
        Task<IList<IFullyEvent<PrimaryKey>>> GetList(PrimaryKey stateId, long latestTimestamp, long startVersion, long endVersion);
        /// <summary>
        /// 批量获取指定类型的事件
        /// </summary>
        /// <param name="stateId">状态Id，相当于GrainId</param>
        /// <param name="typeCode">将要拉取列表的起点时间</param>
        /// <param name="startVersion">开始版本</param>
        /// <param name="limit">拉取数量</param>
        /// <returns></returns>
        Task<IList<IFullyEvent<PrimaryKey>>> GetListByType(PrimaryKey stateId, string typeCode, long startVersion, int limit);
        /// <summary>
        /// 删除指定版本号之前的事件
        /// </summary>
        /// <param name="stateId">状态Id，相当于GrainId</param>
        /// <param name="endVersion">结束版本号</param>
        /// <param name="startTimestamp">当前删除的开始时间戳</param>
        /// <returns></returns>
        Task DeleteStart(PrimaryKey stateId, long endVersion, long startTimestamp);
        /// <summary>
        /// 删除指定版本号之后的事件
        /// </summary>
        /// <param name="stateId">状态Id，相当于GrainId</param>
        /// <param name="startVersion">结束版本号</param>
        /// <param name="startTimestamp">当前删除的开始时间戳</param>
        /// <returns></returns>
        Task DeleteEnd(PrimaryKey stateId, long startVersion, long startTimestamp);
    }
}
