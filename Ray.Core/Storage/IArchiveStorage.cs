using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IArchiveStorage<PrimaryKey, Snapshot>
        where Snapshot : class, new()
    {
        Task Insert(ArchiveBrief brief, Snapshot<PrimaryKey, Snapshot> state);
        Task Delete(string briefId);
        Task DeleteAll(PrimaryKey stateId);
        Task EventIsClear(string briefId);
        Task<Snapshot<PrimaryKey, Snapshot>> GetState(string briefId);
        Task Over(PrimaryKey stateId, bool isOver);
        Task<List<ArchiveBrief>> GetBriefList(PrimaryKey stateId);
        Task<ArchiveBrief> GetLatestBrief(PrimaryKey stateId);
    }
}
