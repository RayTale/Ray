using System.Threading.Tasks;
using Orleans;

namespace Ray.Core.Services
{
    public interface IHoldLock : IGrainWithStringKey
    {
        Task<(bool isOk, long lockId)> Lock(int holdingSeconds =30);
        Task<bool> Hold(long lockId, int holdingSeconds = 30);
        Task Unlock(long lockId);
    }
}
