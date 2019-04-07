using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Ray.Core
{
    public interface IObserver
    {
        Task OnNext(Immutable<byte[]> bytes);
        Task<long> GetVersion();
        Task<long> GetAndSaveVersion(long compareVersion);
    }
}
