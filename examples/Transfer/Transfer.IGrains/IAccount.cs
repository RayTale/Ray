using Orleans;
using Orleans.Concurrency;
using Ray.Core.Event;
using System.Threading.Tasks;

namespace Transfer.IGrains
{
    public interface IAccount: IGrainWithIntegerKey
    {
        /// <summary>
        /// Get account balance
        /// </summary>
        /// <returns></returns>
        [AlwaysInterleave]
        Task<decimal> GetBalance();
        /// <summary>
        /// Increase account amount
        /// </summary>
        /// <param name="amount">amount</param>
        /// <returns></returns>
        Task<bool> TopUp(decimal amount);
        /// <summary>
        /// Final consistent transfer
        /// </summary>
        /// <param name="toAccountId">target account ID</param>
        /// <param name="amount">transfer amount</param>
        /// <returns></returns>
        Task<bool> Transfer(long toAccountId, decimal amount);
        /// <summary>
        /// Transfer to account
        /// </summary>
        /// <param name="amount">Amount to account</param>
        /// <param name="uid">Unique key</param>
        /// <returns></returns>
        Task TransferArrived(decimal amount, EventUID uid);
        /// <summary>
        /// Refund for failed transfer
        /// </summary>
        /// <param name="amount">amount</param>
        /// <param name="uid">Unique key</param>
        /// <returns></returns>
        Task<bool> TransferRefunds(decimal amount, EventUID uid);
    }
}