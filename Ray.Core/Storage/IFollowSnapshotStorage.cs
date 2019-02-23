using System.Threading.Tasks;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IFollowSnapshotStorage<PrimaryKey>
    {
        Task<FollowSnapshot<PrimaryKey>> Get(PrimaryKey id);
        Task Insert(FollowSnapshot<PrimaryKey> data);
        Task Update(FollowSnapshot<PrimaryKey> data);
        Task UpdateStartTimestamp(PrimaryKey id, long timestamp);
        Task Delete(PrimaryKey id);
    }
}
