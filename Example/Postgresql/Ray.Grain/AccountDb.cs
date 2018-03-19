using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains.Actors;
using Ray.IGrains.Events;
using Ray.IGrains.States;
using Ray.Postgresql;

namespace Ray.Grain
{
    public sealed class AccountDb : DbGrain<string, AsyncState<string>>, IAccountDb
    {
        SqlConfig config;
        public AccountDb(IOptions<SqlConfig> configOptions)
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
                    _table = new SqlTable(config.ConnectionDict["core_event"], "account_event", "account_db_state");
                }
                return _table;
            }
        }

        protected override Task Process(IEventBase<string> @event)
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
            Console.WriteLine($"更新数据库->用户转账,当前账户ID:{evt.StateId},目标账户ID:{evt.ToAccountId},转账金额:{evt.Amount},当前余额为:{evt.Balance}");
            return Task.CompletedTask;
        }
        public Task AmountAddEventHandler(AmountAddEvent evt)
        {
            Console.WriteLine($"更新数据库->用户转账到账,用户ID:{evt.StateId},到账金额:{evt.Amount},当前余额为:{evt.Balance}");
            return Task.CompletedTask;
        }
    }
}
