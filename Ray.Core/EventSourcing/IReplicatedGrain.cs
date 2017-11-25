using Orleans;
using Ray.Core;
using System.Threading.Tasks;

namespace Coin.Core.EventSourcing
{
    public interface IReplicatedGrain<W> where W : MessageWrapper
    {
        Task Tell(W message);
    }
}
