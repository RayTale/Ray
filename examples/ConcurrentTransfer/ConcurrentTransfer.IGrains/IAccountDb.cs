using Orleans;
using Ray.Core.Observer;

namespace ConcurrentTransfer.IGrains
{
    public interface IAccountDb : IObserver, IGrainWithIntegerKey
    {
    }
}
