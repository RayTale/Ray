using System;
using Microsoft.Extensions.Options;
using Ray.Core.Storage;
using Ray.Storage.SQLCore.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Storage.SQLServer
{
    public class SQLServerConfigureBuilder<PrimaryKey, Grain> : SQLConfigureBuilder<StorageFactory, PrimaryKey, Grain>
    {
        public SQLServerConfigureBuilder(Func<IServiceProvider, PrimaryKey, DefaultConfigParameter, StorageOptions> generator, bool singleton = true)
            : base(
                            (provider, id, parameter) =>
                        {
                            var result = generator(provider, id, parameter);
                            if (string.IsNullOrEmpty(result.Connection))
                            {
                                result.Connection = provider.GetService<IOptions<SQLServerConnections>>().Value.ConnectionDict[result.ConnectionKey];
                            }

                            if (result.CreateConnectionFunc == default)
                            {
                                result.CreateConnectionFunc = connection => SQLServerFactory.CreateConnection(connection);
                            }

                            if (result.BuildRepository == default)
                            {
                                result.BuildRepository = new SQLServerBuildService(result);
                            }

                            return result;
                        }, singleton)
        {
        }
    }
}
