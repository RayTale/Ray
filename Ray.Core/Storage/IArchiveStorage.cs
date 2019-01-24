using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IArchiveStorage<K, S, B>
        where S : IState<K, B>
        where B : ISnapshot<K>, new()
    {
        Task Insert(ArchiveBrief brief, S state);
        Task Delete(string briefId, K stateId);
        Task EventIsClear(string id);
        Task<ArchiveBrief> GetFirstBrief(K stateId);
        Task<ArchiveBrief> GetLastBrief(K stateId);
        Task<List<ArchiveBrief>> GetBriefList(K stateId);
        Task<SnapshotArchive<K, S, B>> Get(long id);
    }
}
