using System;
using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public interface IConfigureBuilder<PrimaryKey>
    {
        Type StorageFactory { get; }
        ValueTask<IStorageOptions> GetConfig(IServiceProvider serviceProvider, PrimaryKey primaryKey);
        ValueTask<IObserverStorageOptions> GetObserverConfig(IServiceProvider serviceProvider, Type followGrainType, PrimaryKey primaryKey);
    }
    public interface IConfigureBuilder<PrimaryKey, Grain> : IConfigureBuilder<PrimaryKey>
    {
    }
}
