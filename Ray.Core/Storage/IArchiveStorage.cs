using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IArchiveStorage<K, S>
        where S : class, new()
    {
        Task Insert(ArchiveBrief brief, Snapshot<K, S> state);
        Task Delete(string briefId);
        Task DeleteAll(K stateId);
        Task EventIsClear(string briefId);
        Task<Snapshot<K, S>> GetState(string briefId);
        Task UpdateIsLatest(string briefId, bool isLatest);
        Task Over(K stateId, bool isOver);
        Task<List<ArchiveBrief>> GetBriefList(K stateId);
        Task<ArchiveBrief> GetLatestBrief(K stateId);
    }
}
