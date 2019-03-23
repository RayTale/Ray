using System.Threading.Tasks;

namespace Ray.Core
{
    public interface IObserver : IVersionGrain
    {
        Task OnNext(byte[] bytes);
    }
}
