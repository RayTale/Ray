using Coin.Core.EventSourcing;
using Orleans;
using System.Threading.Tasks;

namespace Ray.IGrains.Actors
{
    public interface IAccountReplicated : IReplicatedGrain<MessageInfo>, IGrainObserver, IGrainWithStringKey
    {
        /// <summary>
        /// 获取账户余额
        /// </summary>
        /// <returns></returns>
        Task<decimal> GetBalance();
    }
}
