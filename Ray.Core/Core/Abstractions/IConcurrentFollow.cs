using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Ray.Core
{
    public interface IConcurrentFollow : IVersionGrain
    {
        [AlwaysInterleave]
        Task ConcurrentTell(byte[] bytes);
    }
}
