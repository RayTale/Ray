using Orleans.Concurrency;
using System.Threading.Tasks;

namespace Ray.Core.EventSourcing
{
    public interface IAsyncGrain
    {
        Task Tell(byte[] bytes);
    }
    public interface IAsyncConcurrentGrain
    {
        [AlwaysInterleave]
        Task ConcurrentTell(byte[] bytes);
    }
}
