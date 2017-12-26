using System;
using System.Threading.Tasks;
using Orleans;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using Ray.MongoES;

namespace Ray.Grain
{
    [MongoStorage("Test", "Account")]
    public sealed class AccountReplicated : MongoESReplicatedGrain<String, AccountState, MessageInfo>, IAccountReplicated
    {
        protected override string GrainId => this.GetPrimaryKeyString();
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(this.State.Balance);
        }
    }
}
