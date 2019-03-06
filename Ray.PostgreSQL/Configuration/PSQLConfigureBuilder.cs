using System;
using Ray.Core.Storage;
using Ray.Storage.SQLCore.Configuration;

namespace Ray.Storage.PostgreSQL
{
    public class PSQLConfigureBuilder<PrimaryKey, Grain> : SQLConfigureBuilder<StorageFactory, PrimaryKey, Grain>
    {
        public PSQLConfigureBuilder(Func<IServiceProvider, PrimaryKey, DefaultConfigParameter, StorageOptions> generator, bool singleton = true) :
            base(generator, singleton)
        {
        }
    }
}
