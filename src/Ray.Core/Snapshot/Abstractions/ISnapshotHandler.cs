using Ray.Core.Event;

namespace Ray.Core.Snapshot
{
    public interface ISnapshotHandler<PrimaryKey, Snapshot>
        where Snapshot : class, new()
    {
        void Apply(Snapshot<PrimaryKey, Snapshot> snapshot, FullyEvent<PrimaryKey> fullyEvent);
    }
}
