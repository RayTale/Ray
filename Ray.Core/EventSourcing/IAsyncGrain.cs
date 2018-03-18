using System.Threading.Tasks;

namespace Ray.Core.EventSourcing
{
    public interface IAsyncGrain<W> where W : MessageWrapper
    {
        Task Tell(byte[] bytes);
    }
}
