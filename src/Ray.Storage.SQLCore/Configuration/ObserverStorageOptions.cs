using System.Data.Common;
using System.Threading.Tasks;
using Ray.Core.Storage;

namespace Ray.Storage.SQLCore.Configuration
{
    public class ObserverStorageOptions : IObserverStorageOptions
    {
        private StorageOptions baseConfig;

        public IStorageOptions Config
        {
            get => this.baseConfig;
            set => this.baseConfig = value as StorageOptions;
        }

        public string ObserverName { get; set; }

        public string ObserverSnapshotTable => $"{this.baseConfig.SnapshotTable}_{this.ObserverName}";

        public DbConnection CreateConnection()
        {
            return this.baseConfig.CreateConnection();
        }

        public ValueTask Build()
        {
            return new ValueTask(this.baseConfig.BuildRepository.CreateObserverSnapshotTable(this.ObserverSnapshotTable));
        }
    }
}
