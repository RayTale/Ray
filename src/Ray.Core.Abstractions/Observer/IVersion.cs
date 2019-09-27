using System.Threading.Tasks;

namespace Ray.Core.Observer
{
    public interface IVersion
    {
        Task<long> GetVersion();
        Task<long> GetAndSaveVersion(long compareVersion);
    }
}
