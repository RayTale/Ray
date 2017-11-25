using System.Collections.Generic;
using System.Threading.Tasks;
using System;

namespace Ray.Core.EventSourcing
{
    public interface IEventStorage<K>
    {
        Task<List<EventInfo<K>>> GetListAsync(K stateId, UInt32 startVersion, UInt32 endVersion, DateTime? startTime = null);
        Task<List<EventInfo<K>>> GetListAsync(K stateId, string typeCode, UInt32 startVersion, UInt32 endVersion, DateTime? startTime = null);
        Task<bool> InsertAsync<T>(T data,byte[] bytes, string uniqueId = null, bool needComplate = true) where T : IEventBase<K>;
        Task Complete<T>(T data) where T : IEventBase<K>;
    }
}
