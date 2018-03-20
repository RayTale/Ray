using System;
using System.Threading.Tasks;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using Ray.IGrains.Events;
using Ray.Postgresql;
using Ray.Grain.EventHandles;
using Orleans.Concurrency;
using Ray.RabbitMQ;
using Microsoft.Extensions.Options;

namespace Ray.Grain
{
    [RabbitPub("Account", "account")]
    public sealed class Account : SqlGrain<string, AccountState, IGrains.MessageInfo>, IAccount
    {
        SqlConfig config;
        public Account(IOptions<SqlConfig> configOptions)
        {
            config = configOptions.Value;
        }

        protected override string GrainId => this.GetPrimaryKeyString();

        static IEventHandle _eventHandle = new AccountEventHandle();
        protected override IEventHandle EventHandle => _eventHandle;
        static SqlGrainConfig _table;
        public override SqlGrainConfig ESSQLTable
        {
            get
            {
                if (_table == null)
                {
                    _table = new SqlGrainConfig(config.ConnectionDict["core_event"], "account_event", "account_state");
                }
                return _table;
            }
        }
        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
        }
        public Task Transfer(string toAccountId, decimal amount)
        {
            var evt = new AmountTransferEvent(toAccountId, amount, this.State.Balance - amount);
            return RaiseEvent(evt).AsTask();
        }
        public Task AddAmount(decimal amount, string uniqueId = null)
        {
            var evt = new AmountAddEvent(amount, this.State.Balance + amount);
            return RaiseEvent(evt, uniqueId: uniqueId).AsTask();
        }
        [AlwaysInterleave]
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(State.Balance);
        }
    }
}
