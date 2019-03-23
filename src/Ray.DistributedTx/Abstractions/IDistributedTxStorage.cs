using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.DistributedTransaction
{
    public interface IDistributedTxStorage
    {
        Task<bool> Append<Input>(string unitName, Commit<Input> commit);
        Task<bool> Update(string unitName, long transactionId, TransactionStatus status);
        Task Delete(string unitName, long transactionId);
        Task<IList<Commit<Input>>> GetList<Input>(string unitName);
    }
}
