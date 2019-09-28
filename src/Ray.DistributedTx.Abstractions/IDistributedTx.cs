using System.Threading.Tasks;

namespace Ray.DistributedTx.Abstractions
{
    public interface IDistributedTx
    {
        Task CommitTransaction(long transactionId);
        Task FinishTransaction(long transactionId);
        Task RollbackTransaction(long transactionId);
    }
}
