using System.Threading.Tasks;
using System;
using System.Collections.Generic;

namespace Ray.Core.EventSourcing
{
    public interface IEventStorage<K>
    {
        Task<IList<IEventBase<K>>> GetListAsync(K stateId, Int64 startVersion, Int64 endVersion, DateTime? startTime = null);
        Task<IList<IEventBase<K>>> GetListAsync(K stateId, string typeCode, Int64 startVersion, Int32 limit, DateTime? startTime = null);
        Task<bool> SaveAsync(IEventBase<K> data, byte[] bytes, string uniqueId = null);
    }
}
