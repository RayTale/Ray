using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.IGrains.Events;
using Ray.IGrains.States;
using Ray.PostgreSQL;

namespace Ray.Grain
{
    public sealed class AccountFlow : SqlAsyncGrain<long, AsyncState<long>, MessageInfo>, IAccountFlow
    {
        SqlConfig config;
        public AccountFlow(IOptions<SqlConfig> configOptions)
        {
            config = configOptions.Value;
        }
        protected override long GrainId => this.GetPrimaryKeyLong();

        static SqlGrainConfig _table;
        public override SqlGrainConfig GrainConfig
        {
            get
            {
                if (_table == null)
                {
                    _table = new SqlGrainConfig(config.ConnectionDict["core_event"], "account_event", "account_flow_state");
                }
                return _table;
            }
        }
        protected override bool Concurrent => true;
        protected override Task OnEventDelivered(IEventBase<long> @event)
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
            await toActor.AddAmount(value.Amount, value.GetUniqueId());
        }
    }
}
