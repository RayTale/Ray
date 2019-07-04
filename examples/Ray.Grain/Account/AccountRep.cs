using Ray.Core;
using Ray.Core.Core.Grains;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using System.Threading.Tasks;

namespace Ray.Grain
{
    [Observer(DefaultObserverGroup.primary, null, typeof(Account))]
    public sealed class AccountRep : TxShadowGrain<Account, long, AccountState>, IAccountRep
    {
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(Snapshot.State.Balance);
        }
    }
}
