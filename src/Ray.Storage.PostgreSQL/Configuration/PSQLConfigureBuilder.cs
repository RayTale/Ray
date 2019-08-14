using System;
using Microsoft.Extensions.Options;
using Ray.Core.Storage;
using Ray.Storage.SQLCore.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Ray.Storage.PostgreSQL
{
    public class PSQLConfigureBuilder<PrimaryKey, Grain> : SQLConfigureBuilder<StorageFactory, PrimaryKey, Grain>
    {
        public PSQLConfigureBuilder(Func<IServiceProvider, PrimaryKey, DefaultConfigParameter, StorageOptions> generator, bool singleton = true) :
                        base((provider, id, parameter) =>
                        {
                            var result = generator(provider, id, parameter);
                            result.Connection = provider.GetService<IOptions<PSQLConnections>>().Value.ConnectionDict[result.ConnectionKey];
                            result.CreateConnectionFunc = connection => PSQLFactory.CreateConnection(connection);
                            result.BuildRepository = new PSQLBuildService(result);
                            return result;
                        }, singleton)
        {
        }
    }
}
