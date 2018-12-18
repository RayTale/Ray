using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Ray.Core.Internal
{
    public interface IConcurrentFollowGrain
    {
        [AlwaysInterleave]
        Task ConcurrentTell(byte[] bytes);
    }
}
