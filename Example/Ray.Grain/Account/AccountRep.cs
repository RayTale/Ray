using System.Threading.Tasks;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.IGrains.States;

namespace Ray.Grain
{
    public sealed class AccountRep : RepGrain<long, AccountState, MessageInfo>, IAccountRep
    {
        public override long GrainId => this.GetPrimaryKeyLong();

        protected override ValueTask Apply(AccountState state, IEventBase<long> evt)
        {
            Account.EventHandle.Apply(state, evt);
            return new ValueTask(Task.CompletedTask);
        }

        public Task<decimal> GetBalance()
        {
            return Task.FromResult(State.Balance);
        }
    }
}
