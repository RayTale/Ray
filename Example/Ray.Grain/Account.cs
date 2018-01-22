using System;
using System.Threading.Tasks;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using Ray.IGrains.Events;
using Ray.MongoES;
using Ray.Grain.EventHandles;
using Orleans.Concurrency;
using Ray.PostgresqlES;
using Microsoft.Extensions.Options;

namespace Ray.Grain
{
    [RabbitMQ.RabbitPub("Account", "account")]
    // [MongoStorage("Test", "Account")]
    public sealed class Account : SqlGrain<String, AccountState, IGrains.MessageInfo>, IAccount
    {
        SqlConfig config;
        public Account(IOptions<SqlConfig> configOptions)
        {
            config = configOptions.Value;
        }
        protected override string GrainId => this.GetPrimaryKeyString();

        static IEventHandle _eventHandle = new AccountEventHandle();
        protected override IEventHandle EventHandle => _eventHandle;

        static SqlTable _table;
        public override SqlTable ESSQLTable
        {
            get
            {
                if (_table == null)
                {
                    _table = new SqlTable(config.ConnectionDict["core_event"], "ex_account");
                }
                return _table;
            }
        }

        public override Task OnActivateAsync()
        {
            return base.OnActivateAsync();
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
            return Task.FromResult(this.State.Balance);
        }
    }
}
