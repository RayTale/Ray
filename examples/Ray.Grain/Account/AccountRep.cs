using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core;
using Ray.Core.Core.Grains;
using Ray.Core.Core.Observer;
using Ray.IGrains.Actors;
using Ray.IGrains.States;

namespace Ray.Grain
{
    [Observer(DefaultObserverGroup.primary, typeof(Account))]
    public sealed class AccountRep : TxShadowGrain<Account, long, AccountState>, IAccountRep
    {
        public AccountRep(ILogger<AccountRep> logger) : base(logger)
        {
        }
        public override long GrainId => this.GetPrimaryKeyLong();

        public Task<decimal> GetBalance()
        {
            return Task.FromResult(Snapshot.State.Balance);
        }
    }
}
