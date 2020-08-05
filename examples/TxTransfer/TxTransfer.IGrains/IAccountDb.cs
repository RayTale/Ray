using Orleans;
using Ray.Core.Observer;

namespace TxTransfer.IGrains
{
    public interface IAccountDb : IObserver, IGrainWithIntegerKey
    {
    }
}
