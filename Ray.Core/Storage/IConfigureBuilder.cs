using System;
using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public interface IConfigureBuilder<PrimaryKey, Grain>
    {
        Type StorageFactory { get; }
        ValueTask<IStorageConfig> GetConfig(IServiceProvider serviceProvider, Type type, PrimaryKey primaryKey);
    }
}
