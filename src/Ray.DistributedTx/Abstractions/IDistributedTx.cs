using System.Threading.Tasks;

namespace Ray.DistributedTransaction
{
    public interface IDistributedTx
    {
        Task CommitTransaction(long transactionId);
        Task FinishTransaction(long transactionId);
        Task RollbackTransaction(long transactionId);
    }
}
