using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace Ray.DistributedTx.Abstractions
{
    public interface IDistributedTxUnit<Input, Output> : IGrainWithIntegerKey
    {
        [AlwaysInterleave]
        Task<Output> Ask(Input input);
    }
}
