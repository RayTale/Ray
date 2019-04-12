using Ray.Core.Event;

namespace RushShopping.Grains.Events
{
    public class CreatingSnapshotEvent<TSnapshot> : IEvent
        where TSnapshot : class, new()
    {
        public TSnapshot Snapshot { get; set; }

        public CreatingSnapshotEvent()
        {

        }

        public CreatingSnapshotEvent(TSnapshot snapshot) : this()
        {
            Snapshot = snapshot;
        }
    }
}