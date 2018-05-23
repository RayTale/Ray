using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains.Actors;
using Ray.IGrains.Events;
using Ray.IGrains.States;
using Ray.PostgreSQL;

namespace Ray.Grain
{
    public sealed class AccountDb : DbGrain<long, AsyncState<long>>, IAccountDb
    {
        SqlConfig config;
        public AccountDb(IOptions<SqlConfig> configOptions)
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
                    _table = new SqlGrainConfig(config.ConnectionDict["core_event"], "account_event", "account_db_state");
                }
                return _table;
            }
        }

        protected override Task Process(IEventBase<long> @event)
        {
            switch (@event)
            {
                case AmountAddEvent value: return AmountAddEventHandler(value);
                case AmountTransferEvent value: return AmountTransferEventHandler(value);
                default: return Task.CompletedTask;
            }
        }
        public Task AmountTransferEventHandler(AmountTransferEvent evt)
        {
            //Console.WriteLine($"更新数据库->用户转账,当前账户ID:{evt.StateId},目标账户ID:{evt.ToAccountId},转账金额:{evt.Amount},当前余额为:{evt.Balance}");
            return Task.CompletedTask;
        }
        public Task AmountAddEventHandler(AmountAddEvent evt)
        {
            //Console.WriteLine($"更新数据库->用户转账到账,用户ID:{evt.StateId},到账金额:{evt.Amount},当前余额为:{evt.Balance}");
            return Task.CompletedTask;
        }
    }
}
