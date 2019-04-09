using System.Threading.Tasks;
using Ray.Core.Snapshot;

namespace RushShopping.IGrains
{
    public interface ICrudGrain
    {
        Task Create<TSnapshotType>(TSnapshotType snapshot);

        Task<TSnapshotType> Get<TSnapshotType>();

        Task Update<TSnapshotType>(TSnapshotType snapshotType);

        Task Delete();
    }
}