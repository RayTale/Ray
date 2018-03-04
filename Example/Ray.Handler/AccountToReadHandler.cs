using Ray.Core.Message;
using Ray.RabbitMQ;
using System.Threading.Tasks;
using Ray.IGrains;
using Ray.IGrains.Events;
using System;

namespace Ray.Handler
{
    [RabbitSub("Read", "Account", "account")]
    public sealed class AccountToReadHandler : PSQLToReadHandler<string>
    {
        public AccountToReadHandler(IServiceProvider svProvider) : base(svProvider)
        {
            Register<AmountAddEvent>();
            Register<AmountTransferEvent>();
        }
        public override Task Tell(byte[] bytes, IActorOwnMessage<string> data, MessageInfo msg)
        {
            switch (data)
            {
                case AmountAddEvent value: return AmountAddEventHandler(value);
                case AmountTransferEvent value: return AmountTransferEventHandler(value);
                default: return Task.CompletedTask;
            }
        }
        public Task AmountTransferEventHandler(AmountTransferEvent evt)
        {
            //System.Console.WriteLine($"数据库->用户转账,当前账户ID:{evt.StateId},目标账户ID:{evt.ToAccountId},转账金额:{evt.Amount},当前余额为:{evt.Balance}");
            return Task.CompletedTask;
        }
        public Task AmountAddEventHandler(AmountAddEvent evt)
        {
            //System.Console.WriteLine($"数据库->用户转账到账,用户ID:{evt.StateId},到账金额:{evt.Amount},当前余额为:{evt.Balance}");
            return Task.CompletedTask;
        }
    }
}
