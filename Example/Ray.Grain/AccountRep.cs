using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.Grain.EventHandles;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using Ray.MongoES;
using Ray.PostgresqlES;

namespace Ray.Grain
{
    [MongoStorage("Test", "Account")]
    public sealed class AccountRep : SqlRepGrain<String, AccountState, MessageInfo>, IAccountRep
    {
        SqlConfig config;
        public AccountRep(IOptions<SqlConfig> configOptions)
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
        public Task<decimal> GetBalance()
        {
            return Task.FromResult(this.State.Balance);
        }
    }
}
