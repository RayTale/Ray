using System.Threading.Tasks;
using Ray.Core.Snapshot;

namespace RushShopping.IGrains
{
    public interface ICrudGrain<TSnapshotDto>
        where TSnapshotDto : class, new()
    {
        Task Create(TSnapshotDto snapshot);

        Task<TSnapshotDto> Get();

        Task Update(TSnapshotDto snapshot);

        Task Delete();
    }
}