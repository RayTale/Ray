using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.Event;

namespace Ray.Core.Storage
{
    public interface IEventStorage<K, E>
        where E : IEventBase<K>
    {
        Task<IList<IEvent<K, E>>> GetListAsync(K stateId, long startVersion, long endVersion);
        Task<IList<IEvent<K, E>>> GetListAsync(K stateId, string typeCode, long startVersion, int limit);
        Task<bool> SaveAsync(IEvent<K, E> data, byte[] bytes, string uniqueId = null);
        Task TransactionSaveAsync(List<EventTransmitWrapper<K, E>> list);
    }
}
