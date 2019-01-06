using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Ray.Storage.PostgreSQL;

namespace RayTest.Grains
{
    public class PostgreSQLStorageConfig : IStorageConfig
    {
        readonly IOptions<SqlConfig> options;
        public PostgreSQLStorageConfig(IOptions<SqlConfig> options)
        {
            this.options = options;
        }
        public Task Configure(IConfigContainer container)
        {
            container.CreateBuilder<long>((grain, id) => new SqlGrainConfig(options.Value.ConnectionDict["core_event"], "account_event", "account_state")).
                BindToGrain<Account>();

            return Task.CompletedTask;
        }
    }
}
