using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Orleans;
using Ray.Core.EventSourcing;
using Ray.IGrains;
using Ray.IGrains.Actors;
using Ray.IGrains.States;
using Ray.PostgreSQL;

namespace Ray.Grain
{
    public sealed class AccountRep : SqlRepGrain<long, AccountState, MessageInfo>, IAccountRep
    {
        SqlConfig config;
        public AccountRep(IOptions<SqlConfig> configOptions)
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
                    _table = new SqlGrainConfig(config.ConnectionDict["core_event"], "account_event", "account_state");
                }
                return _table;
            }
        }
        protected override Task Apply(AccountState state, IEventBase<long> evt)
        {
            Account.EventHandle.Apply(state, evt);
            return Task.CompletedTask;
        }

        public Task<decimal> GetBalance()
        {
            return Task.FromResult(State.Balance);
        }
    }
}
