using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Ray.Core.Internal
{
    public interface IInterleaveFollowGrain
    {
        [AlwaysInterleave]
        Task ConcurrentTell(byte[] bytes);
    }
}
