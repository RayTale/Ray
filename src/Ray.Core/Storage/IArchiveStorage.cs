using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.Snapshot;

namespace Ray.Core.Storage
{
    public interface IArchiveStorage<PrimaryKey, StateType>
        where StateType : class, new()
    {
        Task Insert(ArchiveBrief brief, Snapshot<PrimaryKey, StateType> snapshot);
        Task Delete(PrimaryKey stateId, string briefId);
        Task DeleteAll(PrimaryKey stateId);
        Task EventIsClear(PrimaryKey stateId, string briefId);
        Task<Snapshot<PrimaryKey, StateType>> GetById(string briefId);
        Task Over(PrimaryKey stateId, bool isOver);
        Task<List<ArchiveBrief>> GetBriefList(PrimaryKey stateId);
        Task<ArchiveBrief> GetLatestBrief(PrimaryKey stateId);
        Task EventArichive(PrimaryKey stateId, long endVersion, long startTimestamp);
    }
}
