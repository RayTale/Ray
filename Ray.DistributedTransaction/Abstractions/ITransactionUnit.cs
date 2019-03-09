using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace Ray.DistributedTransaction
{
    public interface ITransactionUnit<Input, Output> : IGrainWithIntegerKey
    {
        [AlwaysInterleave]
        Task<Output> Tell(Input input);
    }
}
