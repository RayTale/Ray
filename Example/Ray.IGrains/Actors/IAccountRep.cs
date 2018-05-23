using Orleans;
using System.Threading.Tasks;
using Ray.Core.EventSourcing;

namespace Ray.IGrains.Actors
{
    public interface IAccountRep : IAsyncGrain, IGrainWithIntegerKey
    {
        /// <summary>
        /// 获取账户余额
        /// </summary>
        /// <returns></returns>
        Task<decimal> GetBalance();
    }
}
