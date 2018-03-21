using System.Threading.Tasks;

namespace Ray.Core.EventSourcing
{
    public interface IAsyncGrain
    {
        Task Tell(byte[] bytes);
    }
}
