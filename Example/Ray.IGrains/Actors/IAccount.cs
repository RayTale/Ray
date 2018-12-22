using Orleans;
using Orleans.Concurrency;
using Ray.Core.Internal;
using System.Threading.Tasks;

namespace Ray.IGrains.Actors
{
    public interface IAccount : IGrainWithIntegerKey
    {
        /// <summary>
        /// 获取账户余额
        /// </summary>
        /// <returns></returns>
        [AlwaysInterleave]
        Task<decimal> GetBalance();
        /// <summary>
        /// 增加账户金额
        /// </summary>
        /// <param name="amount">金额</param>
        /// <param name="uniqueId">操作辨识ID(防止多次执行)</param>
        /// <returns></returns>
        [AlwaysInterleave]
        Task<bool> AddAmount(decimal amount, EventUID uniqueId = null);
        /// <summary>
        /// 转账
        /// </summary>
        /// <param name="toAccountId">目标账户ID</param>
        /// <param name="amount">转账金额</param>
        /// <returns></returns>
        Task Transfer(long toAccountId, decimal amount);
    }
}
