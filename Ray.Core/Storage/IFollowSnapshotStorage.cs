using System.Threading.Tasks;
using Ray.Core.Snapshot;

namespace Ray.Core.Storage
{
    public interface IFollowSnapshotStorage<PrimaryKey>
    {
        Task<FollowSnapshot<PrimaryKey>> Get(PrimaryKey id);
        Task Insert(FollowSnapshot<PrimaryKey> snapshot);
        Task Update(FollowSnapshot<PrimaryKey> snapshot);
        Task UpdateStartTimestamp(PrimaryKey id, long timestamp);
        Task Delete(PrimaryKey id);
    }
}
