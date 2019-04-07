using System.Threading.Tasks;
using Ray.Core.Snapshot;

namespace Ray.Core.Storage
{
    public interface IObserverSnapshotStorage<PrimaryKey>
    {
        Task<ObserverSnapshot<PrimaryKey>> Get(PrimaryKey id);
        Task Insert(ObserverSnapshot<PrimaryKey> snapshot);
        Task Update(ObserverSnapshot<PrimaryKey> snapshot);
        Task UpdateStartTimestamp(PrimaryKey id, long timestamp);
        Task Delete(PrimaryKey id);
    }
}
