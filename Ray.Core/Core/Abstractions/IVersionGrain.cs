using System.Threading.Tasks;

namespace Ray.Core
{
    public interface IVersionGrain
    {
        Task<long> CurrentVersion();
    }
}
