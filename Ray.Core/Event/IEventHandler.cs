using Ray.Core.State;

namespace Ray.Core.Event
{
    public interface IEventHandler<PrimaryKey, Snapshot>
        where Snapshot : class, new()
    {
        void Apply(Snapshot<PrimaryKey, Snapshot> state, IFullyEvent<PrimaryKey> evt);
    }
}
