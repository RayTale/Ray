using System.Threading.Tasks;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using Ray.IGrains.Events;
using Ray.PostgreSQL;
using Ray.Grain.EventHandles;
using Ray.RabbitMQ;
using Microsoft.Extensions.Options;
using System;

namespace Ray.Grain
{
    [RabbitPub("Account", "account")]
    public sealed class Account : SqlGrain<long, AccountState, IGrains.MessageInfo>, IAccount
    {
        SqlConfig config;
        public Account(IOptions<SqlConfig> configOptions)
        {
            config = configOptions.Value;
        }

        protected override long GrainId => this.GetPrimaryKeyLong();

        static IEventHandle _eventHandle = new AccountEventHandle();
        protected override IEventHandle EventHandle => _eventHandle;
        static SqlGrainConfig _table;
        protected override bool SupportAsync => false;
        protected override bool OpenInputBuffer => true;
        public override SqlGrainConfig GrainConfig
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
        public Task Transfer(long toAccountId, decimal amount)
        {
            var evt = new AmountTransferEvent(toAccountId, amount, this.State.Balance - amount);
            return RaiseEvent(evt).AsTask();
        }
        public Task AddAmount(decimal amount, string uniqueId = null)
        {
            var evt = new AmountAddEvent(amount);
            return EnterBuffer(evt, uniqueId).AsTask();
        }
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(State.Balance);
        }
    }
}
