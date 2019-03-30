using System.Threading.Tasks;

namespace Ray.Core
{
    public interface IObserver
    {
        Task OnNext(byte[] bytes);
        Task<long> GetVersion();
        Task<long> GetAndSaveVersion(long compareVersion);
    }
}
