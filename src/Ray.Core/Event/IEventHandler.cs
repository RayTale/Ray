using Ray.Core.Snapshot;

namespace Ray.Core.Event
{
    public interface IEventHandler<PrimaryKey, Snapshot>
        where Snapshot : class, new()
    {
        void Apply(Snapshot<PrimaryKey, Snapshot> snapshot, IFullyEvent<PrimaryKey> fullyEvent);
    }
}
