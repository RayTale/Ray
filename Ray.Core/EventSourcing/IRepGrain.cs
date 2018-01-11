using Orleans;
using Ray.Core;
using System.Threading.Tasks;

namespace Coin.Core.EventSourcing
{
    public interface IRepGrain<W> where W : MessageWrapper
    {
        Task Tell(byte[] bytes);
        Task Tell(W message);
    }
}
