using System;
using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ray.Storage.SQLCore.Configuration;
using Ray.Storage.SQLCore.Services;

namespace Ray.Storage.PostgreSQL
{
    public class LongStorageOptions : StorageOptions
    {
        public LongStorageOptions(IServiceProvider serviceProvider, string connectionKey, string uniqueName, long subTableMinutesInterval = 40) : base(serviceProvider)
        {
            Connection = serviceProvider.GetService<IOptions<PSQLConnections>>().Value.ConnectionDict[connectionKey];
            UniqueName = uniqueName;
            SubTableMillionSecondsInterval = subTableMinutesInterval * 24 * 60 * 60 * 1000;
            BuildRepository = new PSQLBuildRepository(this);
        }
        public string Connection { get; set; }
        public override IBuildRepository BuildRepository { get; }

        public override DbConnection CreateConnection()
        {
            return PSQLFactory.CreateConnection(Connection);
        }
    }
}
