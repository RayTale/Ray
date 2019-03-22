using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Ray.Core
{
    public interface IConcurrentObserver : IVersionGrain
    {
        [AlwaysInterleave]
        Task ConcurrentOnNext(byte[] bytes);
    }
}
