using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Ray.Core.Observer
{
    public interface IConcurrentObserver : IVersion
    {
        [AlwaysInterleave]
        Task OnNext(Immutable<byte[]> bytes);
    }
}
