using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;
using Ray.Core.Event;
using Ray.DistributedTx;

namespace Ray.IGrains.Actors
{
    public interface IAccount : IDistributedTx, IGrainWithIntegerKey
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
        /// 最终一致性转账
        /// </summary>
        /// <param name="toAccountId">目标账户ID</param>
        /// <param name="amount">转账金额</param>
        /// <returns></returns>
        Task Transfer(long toAccountId, decimal amount);
        /// <summary>
        /// 分布式事务转账(扣钱)
        /// </summary>
        /// <param name="amount"></param>
        /// <param name="transactionId">事务Id</param>
        /// <returns></returns>
        [AlwaysInterleave]
        Task<bool> TransferDeduct(decimal amount, long transactionId);
        /// <summary>
        /// 分布式事务转账(到账)
        /// </summary>
        /// <param name="amount"></param>
        /// <param name="transactionId">事务Id</param>
        /// <returns></returns>
        [AlwaysInterleave]
        Task TransferAddAmount(decimal amount, long transactionId);
    }
}
