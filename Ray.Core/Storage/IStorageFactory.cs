using System.Threading.Tasks;
using Orleans;
using Ray.Core.Event;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IStorageFactory
    {
        ValueTask<IStateStorage<K, S>> CreateStateStorage<K, S>(Grain grain, K grainId)
            where S : class, IActorState<K>, new();
        ValueTask<IEventStorage<K, E>> CreateEventStorage<K, E, S>(Grain grain, K grainId)
            where E : IEventBase<K>
            where S : class, IActorState<K>, new();
    }
}
