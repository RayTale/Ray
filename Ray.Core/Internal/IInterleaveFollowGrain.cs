using Orleans.Concurrency;
using System.Threading.Tasks;

namespace Ray.Core.Internal
{
    public interface IInterleaveFollowGrain
    {
        [AlwaysInterleave]
        Task ConcurrentTell(byte[] bytes);
    }
}
