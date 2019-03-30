using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.Abstractions;

namespace Ray.Core
{
    public interface IObserverUnit<PrimaryKey> : IGrainID
    {
        Task<long[]> GetAndSaveVersion(PrimaryKey primaryKey, long srcVersion);
        List<Func<byte[], Task>> GetEventHandlers(string followType);
        List<Func<byte[], Task>> GetAllEventHandlers();
    }
}
