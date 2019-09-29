using Orleans;
using Ray.Core.Observer;

namespace Transfer.IGrains
{
    public interface IAccountDb : IObserver, IGrainWithIntegerKey
    {
    }
}
