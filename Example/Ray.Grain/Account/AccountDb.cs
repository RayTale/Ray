using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.Internal;
using Ray.IGrains.Actors;
using Ray.IGrains.Events;
using Ray.IGrains.States;

namespace Ray.Grain
{
    public sealed class AccountDb : DbGrain<long, AsyncState<long>>, IAccountDb
    {
        public AccountDb(ILogger<AccountDb> logger) : base(logger)
        {
        }
        public override long GrainId => this.GetPrimaryKeyLong();

        protected override bool Concurrent => true;
        protected override async ValueTask Process(IEventBase<long> @event)
        {
            switch (@event)
            {
                case AmountAddEvent value: await AmountAddEventHandler(value); break;
                case AmountTransferEvent value: await AmountTransferEventHandler(value); break;
            }
        }
        public Task AmountTransferEventHandler(AmountTransferEvent evt)
        {
            //Console.WriteLine($"更新数据库->用户转账,当前账户ID:{evt.StateId},目标账户ID:{evt.ToAccountId},转账金额:{evt.Amount},当前余额为:{evt.Balance}");
            return Task.Delay(20);
        }
        public Task AmountAddEventHandler(AmountAddEvent evt)
        {
            //Console.WriteLine($"更新数据库->用户转账到账,用户ID:{evt.StateId},到账金额:{evt.Amount},当前余额为:{evt.Balance}");
            return Task.Delay(20);
        }
    }
}
