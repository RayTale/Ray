using System.Threading.Tasks;

namespace Ray.Storage.Mongo.Core
{
    public interface IIndexBuildService
    {
        Task CreateSubTableRecordIndex(ICustomClient client, string database, string collectionName);

        Task CreateSnapshotIndex(ICustomClient client, string database, string collectionName);

        Task CreateSnapshotArchiveIndex(ICustomClient client, string database, string collectionName);

        Task CreateEventIndex(ICustomClient client, string database, string collectionName);

        Task CreateEventArchiveIndex(ICustomClient client, string database, string collectionName);

        Task CreateTransactionStorageIndex(ICustomClient client, string database, string collectionName);
    }
}
