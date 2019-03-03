using System;
using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public interface IConfigureBuilder<PrimaryKey, Grain>
    {
        Type StorageFactory { get; }
        ValueTask<IStorageConfig> GetConfig(IServiceProvider serviceProvider, PrimaryKey primaryKey);
        ValueTask<IFollowStorageConfig> GetFollowConfig(IServiceProvider serviceProvider, Type followGrainType, PrimaryKey primaryKey);
    }
}
