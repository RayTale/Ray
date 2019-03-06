using System;
using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ray.Storage.SQLCore.Configuration;
using Ray.Storage.SQLCore.Services;

namespace Ray.Storage.PostgreSQL
{
    public class StringStorageOptions : StorageOptions
    {
        public StringStorageOptions(IServiceProvider serviceProvider, string connectionKey, string uniqueName, long subTableMinutesInterval = 40, int stateIdLength = 200) : base(serviceProvider)
        {
            StateIdLength = stateIdLength;
            Connection = serviceProvider.GetService<IOptions<PSQLConnections>>().Value.ConnectionDict[connectionKey];
            UniqueName = uniqueName;
            SubTableMillionSecondsInterval = subTableMinutesInterval * 24 * 60 * 60 * 1000;
            BuildRepository = new PSQLBuildRepository(this);
        }
        public int StateIdLength { get; set; }
        public string Connection { get; set; }
        public override IBuildRepository BuildRepository { get; }

        public override DbConnection CreateConnection()
        {
            return PSQLFactory.CreateConnection(Connection);
        }
    }
}
