using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Ray.Core
{
    public interface IObserver : IVersion
    {
        Task OnNext(Immutable<byte[]> bytes);
    }
}
