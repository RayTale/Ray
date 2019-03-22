using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core;
using Ray.IGrains.Actors;
using Ray.IGrains.States;

namespace Ray.Grain
{
    public sealed class AccountRep : ShadowGrain<Account, long, AccountState>, IAccountRep
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
