using System;
using Ray.Core.Event;

namespace RushShopping.Grains.Events
{
    [Serializable]
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