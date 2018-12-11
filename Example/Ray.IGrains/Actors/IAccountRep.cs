using Orleans;
using System.Threading.Tasks;
using Ray.Core.Internal;

namespace Ray.IGrains.Actors
{
    public interface IAccountRep : IFollowGrain, IGrainWithIntegerKey
    {
        /// <summary>
        /// 获取账户余额
        /// </summary>
        /// <returns></returns>
        Task<decimal> GetBalance();
    }
}
