using Orleans;
using System;
using System.Threading.Tasks;

namespace Ray.Core.EventSourcing
{
    public interface IStorageContainer
    {
        ValueTask<IStateStorage<S, K>> GetStateStorage<K, S>(Grain grain) where S : class, IState<K>, new();
        ValueTask<IEventStorage<K>> GetEventStorage<K, S>(Grain grain) where S : class, IState<K>, new();
    }
}
