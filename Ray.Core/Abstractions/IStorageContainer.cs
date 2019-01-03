using System.Threading.Tasks;
using Orleans;

namespace Ray.Core.Abstractions
{
    public interface IStorageContainer
    {
        ValueTask<IStateStorage<S, K>> GetStateStorage<K, S>(Grain grain) where S : class, IState<K>, new();
        ValueTask<IEventStorage<K>> GetEventStorage<K, S>(Grain grain) where S : class, IState<K>, new();
    }
}
