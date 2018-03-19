using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.IGrains.Events;
using Ray.IGrains.States;
using Ray.Postgresql;

namespace Ray.Grain
{
    public sealed class AccountFlow : SqlAsyncGrain<string, AsyncState<string>, MessageInfo>, IAccountFlow
    {
        SqlConfig config;
        public AccountFlow(IOptions<SqlConfig> configOptions)
        {
            config = configOptions.Value;
        }
        protected override string GrainId => this.GetPrimaryKeyString();

        static SqlTable _table;
        public override SqlTable ESSQLTable
        {
            get
            {
                if (_table == null)
                {
                    _table = new SqlTable(config.ConnectionDict["core_event"], "account_event", "account_flow_state");
                }
                return _table;
            }
        }
        protected override Task OnEventDelivered(IEventBase<string> @event)
        {
            switch (@event)
            {
                case AmountTransferEvent value: return AmountAddEventHandler(value);
                default: return Task.CompletedTask;
            }
        }
        public async Task AmountAddEventHandler(AmountTransferEvent value)
        {
            var toActor = GrainFactory.GetGrain<IAccount>(value.ToAccountId);
            await toActor.AddAmount(value.Amount, value.Id);
        }
    }
}
