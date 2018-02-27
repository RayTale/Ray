using Orleans;
using System;

namespace Ray.Core.EventSourcing
{
    public interface IStorageContainer
    {
        IStateStorage<S, K> GetStateStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new();
        IEventStorage<K> GetEventStorage<K, S>(Type type, Grain grain) where S : class, IState<K>, new();
    }
}
