using Orleans;
using Ray.Core.Observer;

namespace TxTransfer.IGrains
{
    public interface IAccountFlow : IObserver, IGrainWithIntegerKey
    {
    }
}
