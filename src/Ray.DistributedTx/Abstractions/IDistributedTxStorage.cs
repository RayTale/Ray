using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.DistributedTx
{
    public interface IDistributedTxStorage
    {
        Task Append<Input>(string unitName, Commit<Input> commit) where Input : class, new();
        Task<bool> Update(string unitName, string transactionId, TransactionStatus status);
        Task Delete(string unitName, string transactionId);
        Task<IList<Commit<Input>>> GetList<Input>(string unitName) where Input : class, new();
    }
}
