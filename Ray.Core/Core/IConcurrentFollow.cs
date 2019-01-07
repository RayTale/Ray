using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Ray.Core
{
    public interface IConcurrentFollow
    {
        [AlwaysInterleave] 
        Task ConcurrentTell(byte[] bytes);
    }
}
