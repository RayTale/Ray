using System.Threading.Tasks;

namespace Ray.DistributedTx.Abstractions
{
    public interface IDistributedTx
    {
        Task CommitTransaction(string transactionId);

        Task FinishTransaction(string transactionId);

        Task RollbackTransaction(string transactionId);
    }
}
