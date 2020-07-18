using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public interface IStorageFactory
    {
        ValueTask<IEventStorage<PrimaryKey>> CreateEventStorage<PrimaryKey>(IStorageOptions config, PrimaryKey grainId);
        ValueTask<ISnapshotStorage<PrimaryKey, State>> CreateSnapshotStorage<PrimaryKey, State>(IStorageOptions config, PrimaryKey grainId)
            where State : class, new();
        ValueTask<IObserverSnapshotStorage<PrimaryKey>> CreateObserverSnapshotStorage<PrimaryKey>(IObserverStorageOptions config, PrimaryKey grainId);
        ValueTask<IArchiveStorage<PrimaryKey, State>> CreateArchiveStorage<PrimaryKey, State>(IStorageOptions config, PrimaryKey grainId)
            where State : class, new();
    }
}
