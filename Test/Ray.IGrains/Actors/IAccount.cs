using Orleans;
using System.Threading.Tasks;

namespace Ray.IGrains.Actors
{
    public interface IAccount : IGrainWithStringKey
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
        /// <param name="uniqueId">操作辨识ID(防止多次执行)</param>
        /// <returns></returns>
        Task AddAmount(decimal amount, string uniqueId = null);
        /// <summary>
        /// 转账
        /// </summary>
        /// <param name="toAccountId">目标账户ID</param>
        /// <param name="amount">转账金额</param>
        /// <returns></returns>
        Task Transfer(string toAccountId, decimal amount);
    }
}
