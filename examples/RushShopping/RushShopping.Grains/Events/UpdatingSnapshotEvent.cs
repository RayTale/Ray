using System;
using Ray.Core.Event;

namespace RushShopping.Grains.Events
{
    [Serializable]
    public class UpdatingSnapshotEvent<TSnapshot> : IEvent
        where TSnapshot : class, new()
    {
        public TSnapshot Snapshot { get; set; }

        public UpdatingSnapshotEvent()
        {

        }

        public UpdatingSnapshotEvent(TSnapshot snapshot) : this()
        {
            Snapshot = snapshot;
        }
    }
}