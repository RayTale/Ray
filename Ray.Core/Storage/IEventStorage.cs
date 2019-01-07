using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.Event;

namespace Ray.Core.Storage
{
    public interface IEventStorage<K>
    {
        Task<IList<IEventBase<K>>> GetListAsync(K stateId, long startVersion, long endVersion);
        Task<IList<IEventBase<K>>> GetListAsync(K stateId, string typeCode, long startVersion, int limit);
        Task<bool> SaveAsync(IEventBase<K> data, byte[] bytes, string uniqueId = null);
        Task TransactionSaveAsync(List<EventTransmitWrapper<K>> list);
    }
}
