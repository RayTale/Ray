using System.Threading.Tasks;
using Orleans;

namespace Ray.Core.Abstractions.Actors
{
    public interface IUID : IGrainWithStringKey
    {
        Task<long> NewLongID();
        Task<string> NewUtcID();
        Task<string> NewLocalID();
    }
}
