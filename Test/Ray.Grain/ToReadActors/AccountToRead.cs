using System.Threading.Tasks;
using Orleans;
using Ray.Core.Message;
using Ray.IGrains;
using Ray.IGrains.Events;
using Ray.IGrains.States;
using Ray.IGrains.ToReadActors;
using Ray.MongoES;

namespace Ray.Grain.ToReadActors
{
    [MongoStorage("Test", "Account")]
    public class AccountToRead : PSQLToReadGrain<string, ToReadState<string>>, IAccountToRead
    {
        protected override string GrainId => this.GetPrimaryKeyString();
        protected override Task Execute(IMessage msg)
        {
            switch (msg)
            {
                case AmountAddEvent value: return AmountAddEventHandler(value);
                case AmountTransferEvent value: return AmountTransferEventHandler(value);
                default: return Task.CompletedTask;
            }
        }
        public Task AmountTransferEventHandler(AmountTransferEvent value)
        {
            System.Console.WriteLine($"数据库->用户转账,当前账户ID:{value.StateId},目标账户ID:{value.ToAccountId},转账金额:{value.Amount},当前余额为:{value.Balance}");
            return Task.CompletedTask;
        }
        public Task AmountAddEventHandler(AmountAddEvent value)
        {
            System.Console.WriteLine($"数据库->用户转账到账,用户ID:{value.StateId},到账金额:{value.Amount},当前余额为:{value.Balance}");
            return Task.CompletedTask;
        }
    }
}
