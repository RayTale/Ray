using AutoMapper;
using Ray.Core.Event;
using Ray.Core.Snapshot;
using Ray.DistributedTransaction;
using RushShopping.Grains.Events;

namespace RushShopping.Grains
{
    public class CrudHandle<TPrimaryKey, TSnapshot> : TxEventHandler<TPrimaryKey, TSnapshot>,
        ICrudHandle<TPrimaryKey, TSnapshot>
        where TSnapshot : class, new()
    {
        protected readonly IMapper Mapper;

        public CrudHandle(IMapper mapper)
        {
            Mapper = mapper;
        }

        public override void CustomApply(Snapshot<TPrimaryKey, TSnapshot> snapshot, IFullyEvent<TPrimaryKey> fullyEvent)
        {
            Apply(snapshot.State, fullyEvent.Event);
        }

        public void CreatingSnapshotHandle(TSnapshot snapshotState, CreatingSnapshotEvent<TSnapshot> evt)
        {
            Mapper.Map(evt.Snapshot, snapshotState);
        }

        public void UpdatingSnapshotHandle(TSnapshot snapshotState, UpdatingSnapshotEvent<TSnapshot> evt)
        {
            Mapper.Map(evt.Snapshot, snapshotState);
        }

        public void DeletingSnapshotHandle(TSnapshot snapshotState, DeletingSnapshotEvent<TPrimaryKey> evt)
        {
            var defaultSnapshot = new TSnapshot();
            Mapper.Map(defaultSnapshot, snapshotState);
        }

        #region Implementation of ICrudHandle<in TSnapshot>

        public virtual void Apply(TSnapshot snapshot, IEvent @event)
        {
            switch (@event)
            {
                case CreatingSnapshotEvent<TSnapshot> evt:
                    CreatingSnapshotHandle(snapshot, evt);
                    break;
                case UpdatingSnapshotEvent<TSnapshot> evt:
                    UpdatingSnapshotHandle(snapshot, evt);
                    break;
                case DeletingSnapshotEvent<TPrimaryKey> evt:
                    DeletingSnapshotHandle(snapshot, evt);
                    break;
            }
        }

        #endregion
    }
}