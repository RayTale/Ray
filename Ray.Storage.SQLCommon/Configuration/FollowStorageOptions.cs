using System.Data.Common;
using System.Threading.Tasks;
using Ray.Core.Storage;

namespace Ray.Storage.SQLCore.Configuration
{
    public class FollowStorageOptions : IFollowStorageOptions
    {
        StorageOptions _baseConfig;
        public IStorageOptions Config
        {
            get => _baseConfig;
            set
            {
                _baseConfig = value as StorageOptions;
            }
        }
        public string FollowName { get; set; }
        public string FollowSnapshotTable => $"{_baseConfig.SnapshotTable}_{FollowName}";
        public DbConnection CreateConnection()
        {
            return _baseConfig.CreateConnection();
        }
        public ValueTask Build()
        {
            return new ValueTask(_baseConfig.BuildRepository.CreateFollowSnapshotTable(FollowSnapshotTable));
        }
    }
}
