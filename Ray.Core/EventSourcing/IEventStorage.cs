using System.Collections.Generic;
using System.Threading.Tasks;
using System;

namespace Ray.Core.EventSourcing
{
    public interface IEventStorage<K>
    {
        Task<List<EventInfo<K>>> GetListAsync(K stateId, Int64 startVersion, Int64 endVersion, DateTime? startTime = null);
        Task<List<EventInfo<K>>> GetListAsync(K stateId, string typeCode, Int64 startVersion, Int64 endVersion, DateTime? startTime = null);
        Task<bool> SaveAsync<T>(T data, byte[] bytes, string uniqueId = null) where T : IEventBase<K>;
        Task CompleteAsync<T>(T data) where T : IEventBase<K>;
    }
}
