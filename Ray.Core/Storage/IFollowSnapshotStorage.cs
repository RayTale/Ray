using System.Threading.Tasks;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IFollowSnapshotStorage<K>
    {
        Task<FollowSnapshot<K>> Get(K id);

        Task Insert(FollowSnapshot<K> data);

        Task Update(FollowSnapshot<K> data);
        Task UpdateStartTimestamp(K id, long timestamp);
        Task Delete(K id);
    }
}
