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
        Task<S> GetState(string briefId, K stateId);
        Task UpdateIsLatest(string briefId, K stateId, bool isLatest);
        Task<List<ArchiveBrief>> GetBriefList(K stateId);
    }
}
