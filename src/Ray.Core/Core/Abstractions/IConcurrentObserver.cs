using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Ray.Core
{
    public interface IConcurrentObserver : IObserver
    {
        [AlwaysInterleave]
        Task ConcurrentOnNext(Immutable<byte[]> bytes);
    }
}
