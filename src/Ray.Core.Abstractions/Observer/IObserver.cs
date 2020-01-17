using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Ray.Core.Observer
{
    public interface IObserver : IVersion
    {
        Task OnNext(Immutable<byte[]> bytes);
        Task OnNext(Immutable<List<byte[]>> items);
        /// <summary>
        /// 重置状态
        /// </summary>
        /// <returns></returns>
        Task Reset();
    }
}
