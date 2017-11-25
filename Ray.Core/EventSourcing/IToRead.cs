using Ray.Core;
using System.Threading.Tasks;

namespace Coin.Core.EventSourcing
{
    public interface IToRead<W> where W : MessageWrapper
    {
        Task Tell(W msg);
        Task Tell(byte[] bytes);
    }
}
