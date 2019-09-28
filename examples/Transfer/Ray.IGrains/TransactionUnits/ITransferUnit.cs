using Ray.DistributedTx;
using Ray.IGrains.TransactionUnits.Inputs;

namespace Ray.IGrains.TransactionUnits
{
    public interface ITransferUnit : IDistributedTxUnit<TransferInput, bool>
    {
    }
}
