using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IArchiveStorage<K, S, B>
        where S : IState<K, B>
        where B : IStateBase<K>, new()
    {
        Task Insert(BriefArchive brief, S state);
        Task Delete(string briefId, K stateId);
        Task EventIsClear(string id);
        Task<BriefArchive> GetFirstBrief(K stateId);
        Task<BriefArchive> GetLastBrief(K stateId);
        Task<List<BriefArchive>> GetBriefList(K stateId);
        Task<IStateArchive<K, S, B>> Get(long id);
    }
}
