using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public interface IStorageFactory
    {
        ValueTask<IEventStorage<PrimaryKey>> CreateEventStorage<PrimaryKey>(IStorageConfig config, PrimaryKey grainId);
        ValueTask<ISnapshotStorage<PrimaryKey, State>> CreateSnapshotStorage<PrimaryKey, State>(IStorageConfig config, PrimaryKey grainId)
            where State : class, new();
        ValueTask<IFollowSnapshotStorage<PrimaryKey>> CreateFollowSnapshotStorage<PrimaryKey>(IFollowStorageConfig config, PrimaryKey grainId);
        ValueTask<IArchiveStorage<PrimaryKey, State>> CreateArchiveStorage<PrimaryKey, State>(IStorageConfig config, PrimaryKey grainId)
            where State : class, new();
    }
}
