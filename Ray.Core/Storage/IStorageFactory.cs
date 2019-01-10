using System.Threading.Tasks;
using Orleans;
using Ray.Core.State;

namespace Ray.Core.Storage
{
    public interface IStorageFactory
    {
        ValueTask<IStateStorage<K, S>> CreateStateStorage<K, S>(Grain grain, K grainId) where S : class, IState<K>, new();
        ValueTask<IEventStorage<K>> CreateEventStorage<K, S>(Grain grain, K grainId) where S : class, IState<K>, new();
    }
}
