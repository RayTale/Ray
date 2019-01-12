using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IArchiveStorage<K, S>
        where S : IActorState<K>
    {
        Task Append(IStateArchive<K, S> archive);
        Task Delete(long id);
        Task<ArchiveInfo> GetLastBrief(K stateId);
        Task<List<ArchiveInfo>> GetBriefList(K stateId);
        Task<IStateArchive<K, S>> Get(long id);
    }
}
