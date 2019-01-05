using System.Threading.Tasks;
using Orleans;

namespace Ray.Core.Abstractions.Actors
{
    public interface ILock : IGrainWithStringKey
    {
        Task<bool> Lock(int millisecondsDelay = 0);
        Task Unlock();
    }
}
