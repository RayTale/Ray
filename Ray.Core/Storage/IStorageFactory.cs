using System.Threading.Tasks;
using Orleans;
using Ray.Core.Event;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IStorageFactory
    {
        ValueTask<ISnapshotStorage<K, S, B>> CreateSnapshotStorage<K, S, B>(Grain grain, K grainId)
            where S : class, IState<K, B>, new()
            where B : ISnapshot<K>, new();
        ValueTask<IArchiveStorage<K, S, B>> CreateArchiveStorage<K, S, B>(Grain grain, K grainId)
            where S : class, IState<K, B>, new()
            where B : ISnapshot<K>, new();
        ValueTask<IEventStorage<K, E>> CreateEventStorage<K, E>(Grain grain, K grainId)
            where E : IEventBase<K>;
    }
}
