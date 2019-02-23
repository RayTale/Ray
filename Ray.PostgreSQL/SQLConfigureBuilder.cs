using System;
using Ray.Core.Storage;

namespace Ray.Storage.PostgreSQL
{
    public class SQLConfigureBuilder<PrimaryKey, Grain> : ConfigureBuilder<PrimaryKey, Grain, StorageConfig, DefaultConfigParameter>
    {
        readonly bool singleton;
        public SQLConfigureBuilder(Func<IServiceProvider, PrimaryKey, DefaultConfigParameter, StorageConfig> generator, bool singleton = true) : base(generator)
        {
            this.singleton = singleton;
            ParameterDict.Add(typeof(Grain), new DefaultConfigParameter(singleton, false));
        }

        public override Type StorageFactory => typeof(StorageFactory);

        public SQLConfigureBuilder<PrimaryKey, Grain> Follow<T>(string followName = null)
            where T : Orleans.Grain
        {
            Bind<T>(new DefaultConfigParameter(singleton, true, followName));
            return this;
        }
    }
}
