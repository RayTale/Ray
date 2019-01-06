using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Ray.Storage.PostgreSQL;

namespace Ray.Grain
{
    public class PostgreSQLStorageConfig : IStorageConfig
    {
        readonly IOptions<SqlConfig> options;
        public PostgreSQLStorageConfig(IOptions<SqlConfig> options) => this.options = options;
        public Task Configure(IConfigContainer container)
        {
            container.CreateBuilder<long>((grain, id) => new SqlGrainConfig(options.Value.ConnectionDict["core_event"], "account_event", "account_state")).
                BindToGrain<Account>().BindToGrain<AccountRep>().BindToGrain<AccountDb>("account_db_state").BindToGrain<AccountFlow>("account_flow_state").Enable();

            return Task.CompletedTask;
        }
    }
}
