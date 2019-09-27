using System.Threading.Tasks;
using Orleans;


namespace Ray.Core.Services
{
    public interface IWeightHoldLock : IGrainWithStringKey
    {
        Task<(bool isOk, long lockId, int expectMillisecondDelay)> Lock(int weight, int holdingSeconds = 30);
        Task<bool> Hold(long lockId, int holdingSeconds = 30);
        Task Unlock(long lockId);
    }
}
