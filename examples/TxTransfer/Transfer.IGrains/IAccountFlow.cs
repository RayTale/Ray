using Orleans;
using Ray.Core.Observer;

namespace Transfer.IGrains
{
    public interface IAccountFlow : IObserver, IGrainWithIntegerKey
    {
    }
}
