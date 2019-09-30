using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Ray.Core.Observer
{
    public interface IObserver : IVersion
    {
        Task OnNext(Immutable<byte[]> bytes);
        Task OnNext(Immutable<List<byte[]>> items);
    }
}
