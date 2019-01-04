using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace Ray.Core.Abstractions.Actors
{
    public interface IUID : IGrainWithStringKey
    {
        [AlwaysInterleave]
        Task<long> NewLongID();
        Task<string> NewUtcID();
        Task<string> NewLocalID();
    }
}
