using System.Threading.Tasks;

namespace Ray.Core
{
    public interface IVersionGrain
    {
        Task<long> GetVersion();
        Task<long> GetAndSaveVersion(long compareVersion);
    }
}
