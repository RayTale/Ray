using System.Threading.Tasks;
using Orleans;

namespace Ray.Core.Storage
{
    public interface IStorageFactory
    {
        ValueTask<ISnapshotStorage<PrimaryKey, State>> CreateSnapshotStorage<PrimaryKey, State>(Grain grain, PrimaryKey grainId)
            where State : class, new();
        ValueTask<IFollowSnapshotStorage<PrimaryKey>> CreateFollowSnapshotStorage<PrimaryKey>(Grain grain, PrimaryKey grainId);
        ValueTask<IArchiveStorage<PrimaryKey, State>> CreateArchiveStorage<PrimaryKey, State>(Grain grain, PrimaryKey grainId)
            where State : class, new();
        ValueTask<IEventStorage<PrimaryKey>> CreateEventStorage<PrimaryKey>(Grain grain, PrimaryKey grainId);
    }
}
