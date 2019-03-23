using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.Abstractions;

namespace Ray.Core
{
    public interface IObserverUnit<PrimaryKey> : IGrainID
    {
        List<Func<PrimaryKey, long, Task<long>>> GetAndSaveVersionFuncs();
        List<Func<byte[], Task>> GetEventHandlers(string followType);
        List<Func<byte[], Task>> GetAllEventHandlers();
    }
}
