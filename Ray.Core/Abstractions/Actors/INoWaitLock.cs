using System.Threading.Tasks;
using Orleans;

namespace Ray.Core.Abstractions.Actors
{
    public interface INoWaitLock : IGrainWithStringKey
    {
        Task<(bool isOk, long lockId)> Lock(long holdingSeconds=30);
        Task<bool> Hold(long lockId, long holdingSeconds = 30);
        Task Unlock();
    }
}
