using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IArchiveStorage<K, S, B>
        where S : IState<K, B>
        where B : IStateBase<K>, new()
    {
        Task Insert(IStateArchive<K, S, B> archive);
        Task Delete(long id);
        Task<ArchiveInfo> GetLastBrief(K stateId);
        Task<List<ArchiveInfo>> GetBriefList(K stateId);
        Task<IStateArchive<K, S, B>> Get(long id);
    }
}
