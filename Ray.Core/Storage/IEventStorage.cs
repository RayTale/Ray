using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.Event;

namespace Ray.Core.Storage
{
    public interface IEventStorage<PrimaryKey>
    {
        Task<IList<IFullyEvent<PrimaryKey>>> GetList(PrimaryKey stateId, long latestTimestamp, long startVersion, long endVersion);
        Task<IList<IFullyEvent<PrimaryKey>>> GetListByType(PrimaryKey stateId, string typeCode, long startVersion, int limit);
        Task<bool> Append(SaveTransport<PrimaryKey> transport);
        Task DeleteStart(PrimaryKey stateId, long endVersion, long startTimestamp);
        Task DeleteEnd(PrimaryKey stateId, long startVersion, long startTimestamp);
        Task TransactionBatchAppend(List<TransactionTransport<PrimaryKey>> list);
    }
}
