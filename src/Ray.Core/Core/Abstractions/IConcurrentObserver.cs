using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Ray.Core
{
    public interface IConcurrentObserver
    {
        [AlwaysInterleave]
        Task OnNext(Immutable<byte[]> bytes);
    }
}
