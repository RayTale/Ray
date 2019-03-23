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
        /// 批量时间插入
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        Task TransactionBatchAppend(List<EventTransport<PrimaryKey>> list);
        Task<IList<IFullyEvent<PrimaryKey>>> GetList(PrimaryKey stateId, long latestTimestamp, long startVersion, long endVersion);
        Task<IList<IFullyEvent<PrimaryKey>>> GetListByType(PrimaryKey stateId, string typeCode, long startVersion, int limit);
        Task DeleteStart(PrimaryKey stateId, long endVersion, long startTimestamp);
        Task DeleteEnd(PrimaryKey stateId, long startVersion, long startTimestamp);
    }
}
