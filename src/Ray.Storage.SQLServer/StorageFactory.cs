using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Ray.Core.Serialization;
using Ray.Core.Storage;
using Ray.Storage.SQLCore.Configuration;

namespace Ray.Storage.SQLServer
{
    public class StorageFactory : IStorageFactory
    {
        private readonly IServiceProvider serviceProvider;
        private readonly ISerializer serializer;

        public StorageFactory(
            IServiceProvider serviceProvider,
            ISerializer serializer)
        {
            this.serializer = serializer;
            this.serviceProvider = serviceProvider;
        }

        private readonly ConcurrentDictionary<IStorageOptions, object> eventStorageDict = new ConcurrentDictionary<IStorageOptions, object>();

        public ValueTask<IEventStorage<PrimaryKey>> CreateEventStorage<PrimaryKey>(IStorageOptions config, PrimaryKey grainId)
        {
            if (config.Singleton)
            {
                var storage = this.eventStorageDict.GetOrAdd(config, key =>
                {
                    return new EventStorage<PrimaryKey>(this.serviceProvider, config as StorageOptions);
                });
                return new ValueTask<IEventStorage<PrimaryKey>>(storage as EventStorage<PrimaryKey>);
            }
            else
            {
                return new ValueTask<IEventStorage<PrimaryKey>>(new EventStorage<PrimaryKey>(this.serviceProvider, config as StorageOptions));
            }
        }

        private readonly ConcurrentDictionary<IStorageOptions, object> stateStorageDict = new ConcurrentDictionary<IStorageOptions, object>();

        public ValueTask<ISnapshotStorage<PrimaryKey, State>> CreateSnapshotStorage<PrimaryKey, State>(IStorageOptions config, PrimaryKey grainId)
            where State : class, new()
        {
            if (config.Singleton)
            {
                var storage = this.stateStorageDict.GetOrAdd(config, key =>
                {
                    return new SnapshotStorage<PrimaryKey, State>(this.serializer, config as StorageOptions);
                });
                return new ValueTask<ISnapshotStorage<PrimaryKey, State>>(storage as SnapshotStorage<PrimaryKey, State>);
            }
            else
            {
                return new ValueTask<ISnapshotStorage<PrimaryKey, State>>(new SnapshotStorage<PrimaryKey, State>(this.serializer, config as StorageOptions));
            }
        }

        private readonly ConcurrentDictionary<IStorageOptions, object> ArchiveStorageDict = new ConcurrentDictionary<IStorageOptions, object>();

        public ValueTask<IArchiveStorage<PrimaryKey, State>> CreateArchiveStorage<PrimaryKey, State>(IStorageOptions config, PrimaryKey grainId)
             where State : class, new()
        {
            if (config.Singleton)
            {
                var storage = this.ArchiveStorageDict.GetOrAdd(config, key =>
                {
                    return new ArchiveStorage<PrimaryKey, State>(this.serviceProvider, this.serializer, config as StorageOptions);
                });
                return new ValueTask<IArchiveStorage<PrimaryKey, State>>(storage as IArchiveStorage<PrimaryKey, State>);
            }
            else
            {
                return new ValueTask<IArchiveStorage<PrimaryKey, State>>(new ArchiveStorage<PrimaryKey, State>(this.serviceProvider, this.serializer, config as StorageOptions));
            }
        }

        private readonly ConcurrentDictionary<IObserverStorageOptions, object> ObserverSnapshotStorageDict = new ConcurrentDictionary<IObserverStorageOptions, object>();

        public ValueTask<IObserverSnapshotStorage<PrimaryKey>> CreateObserverSnapshotStorage<PrimaryKey>(IObserverStorageOptions config, PrimaryKey grainId)
        {
            if (config.Config.Singleton)
            {
                var storage = this.ObserverSnapshotStorageDict.GetOrAdd(config, key =>
                {
                    return new ObserverSnapshotStorage<PrimaryKey>(config as ObserverStorageOptions);
                });
                return new ValueTask<IObserverSnapshotStorage<PrimaryKey>>(storage as IObserverSnapshotStorage<PrimaryKey>);
            }
            else
            {
                return new ValueTask<IObserverSnapshotStorage<PrimaryKey>>(new ObserverSnapshotStorage<PrimaryKey>(config as ObserverStorageOptions));
            }
        }
    }
}
