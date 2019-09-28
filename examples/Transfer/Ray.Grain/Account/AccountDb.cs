using Ray.Core;
using Ray.Core.Event;
using Ray.Grain.Events;
using Ray.IGrains.Actors;
using System.Threading.Tasks;

namespace Ray.Grain
{
    [Observer(DefaultObserverGroup.secondary, "db", typeof(Account))]
    public sealed class AccountDb : DbGrain<long, Account>, IAccountDb
    {
        protected override bool ConcurrentHandle => true;
        public Task EventHandler(AmountTransferEvent evt, EventBase eventBase)
        {
            //Console.WriteLine($"更新数据库->用户转账,当前账户ID:{evt.StateId},目标账户ID:{evt.ToAccountId},转账金额:{evt.Amount},当前余额为:{evt.Balance}");
            return Task.CompletedTask;
        }
        public Task EventHandler(AmountAddEvent evt, EventBase eventBase)
        {
            //Console.WriteLine($"更新数据库->用户转账到账,用户ID:{evt.StateId},到账金额:{evt.Amount},当前余额为:{evt.Balance}");
            return Task.CompletedTask;
        }
        public Task EventHandle(AmountDeductEvent evt)
        {
            return Task.CompletedTask;
        }
    }
}
