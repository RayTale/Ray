using Orleans;
using Ray.Core.Event;
using System.Threading.Tasks;

namespace RayTest.IGrains
{
    public interface IAccount : IGrainWithIntegerKey
    {
        /// <summary>
        /// 获取账户余额
        /// </summary>
        /// <returns></returns>
        Task<decimal> GetBalance();
        /// <summary>
        /// 增加账户金额
        /// </summary>
        /// <param name="amount">金额</param>
        /// <returns></returns>
        Task<bool> TopUp(decimal amount);
        /// <summary>
        /// 最终一致性转账
        /// </summary>
        /// <param name="toAccountId">目标账户ID</param>
        /// <param name="amount">转账金额</param>
        /// <returns></returns>
        Task<bool> Transfer(long toAccountId, decimal amount);
        /// <summary>
        /// 转账到账
        /// </summary>
        /// <param name="amount">到账金额</param>
        /// <param name="uid">唯一键</param>
        /// <returns></returns>
        Task TransferArrived(decimal amount, EventUID uid);
        /// <summary>
        /// 转账失败退款
        /// </summary>
        /// <param name="amount">金额</param>
        /// <param name="uid">唯一键</param>
        /// <returns></returns>
        Task<bool> TransferRefunds(decimal amount, EventUID uid);
    }
}
