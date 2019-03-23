using System.Threading.Tasks;
using Orleans;

namespace Ray.Core.Services.Abstractions
{
    public interface ILock : IGrainWithStringKey
    {
        Task<bool> Lock(int millisecondsDelay = 0);
        Task Unlock();
    }
}
