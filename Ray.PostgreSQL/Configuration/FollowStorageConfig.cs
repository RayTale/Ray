using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core.Storage;
using Ray.Storage.PostgreSQL.Services.Abstractions;

namespace Ray.Storage.PostgreSQL
{
    public class FollowStorageConfig : IFollowStorageConfig
    {
        private readonly ITableRepository tableRepository;
        public FollowStorageConfig(IServiceProvider serviceProvider)
        {
            tableRepository = serviceProvider.GetService<ITableRepository>();
        }
        StorageConfig _baseConfig;
        public IStorageConfig Config
        {
            get => _baseConfig;
            set
            {
                _baseConfig = value as StorageConfig;
            }
        }
        public string FollowName { get; set; }
        public string FollowSnapshotTable => $"{_baseConfig.SnapshotTable}_{FollowName}";

        public ValueTask Init()
        {
            return new ValueTask(tableRepository.CreateFollowSnapshotTable(_baseConfig.Connection, FollowSnapshotTable, _baseConfig.StateIdLength));
        }
    }
}
