using System.Threading.Tasks;

namespace Ray.Core.EventSourcing
{
    public interface IRepGrain<W> where W : MessageWrapper
    {
        Task Tell(byte[] bytes);
        Task Tell(W message);
    }
}
