using System;
using System.Threading.Tasks;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.Grain.EventHandles;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using Ray.MongoDb;

namespace Ray.Grain
{
    [MongoStorage("Test", "Account_Event", "Account_State")]
    public sealed class AccountRep : MongoRepGrain<String, AccountState, MessageInfo>, IAccountRep
    {
        protected override string GrainId => this.GetPrimaryKeyString();

        static IEventHandle _eventHandle = new AccountEventHandle();
        protected override IEventHandle EventHandle => _eventHandle;

        public Task<decimal> GetBalance()
        {
            return Task.FromResult(State.Balance);
        }
    }
}
