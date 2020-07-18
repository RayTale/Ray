using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.DistributedTx
{
    public interface IDistributedTxStorage
    {
        Task Append<TInput>(string unitName, Commit<TInput> commit)
            where TInput : class, new();

        Task<bool> Update(string unitName, string transactionId, TransactionStatus status);

        Task Delete(string unitName, string transactionId);

        Task<IList<Commit<TInput>>> GetList<TInput>(string unitName)
            where TInput : class, new();
    }
}
