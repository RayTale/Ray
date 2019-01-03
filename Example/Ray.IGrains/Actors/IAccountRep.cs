using System.Threading.Tasks;
using Orleans;
using Ray.Core.Abstractions;

namespace Ray.IGrains.Actors
{
    public interface IAccountRep : IFollow, IGrainWithIntegerKey
    {
        /// <summary>
        /// 获取账户余额
        /// </summary>
        /// <returns></returns>
        Task<decimal> GetBalance();
    }
}
