using System.Data.Common;
using System.Threading.Tasks;
using Ray.Core.Storage;

namespace Ray.Storage.SQLCore.Configuration
{
    public class ObserverStorageOptions : IObserverStorageOptions
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
        public string ObserverName { get; set; }
        public string ObserverSnapshotTable => $"{_baseConfig.SnapshotTable}_{ObserverName}";
        public DbConnection CreateConnection()
        {
            return _baseConfig.CreateConnection();
        }
        public ValueTask Build()
        {
            return new ValueTask(_baseConfig.BuildRepository.CreateObserverSnapshotTable(ObserverSnapshotTable));
        }
    }
}
