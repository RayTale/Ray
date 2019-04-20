﻿using System;
using System.Threading.Tasks;
using AutoMapper;
using Ray.Core.Event;
using Ray.Core.Snapshot;
using Ray.DistributedTransaction;
using RushShopping.Grains.Events;

namespace RushShopping.Grains
{
    public abstract class CrudHandle<TSnapshot> : TxEventHandler<Guid, TSnapshot>, ICrudHandle<TSnapshot>
        where TSnapshot : class, new()
    {
        protected readonly IMapper Mapper;

        protected CrudHandle(IMapper mapper)
        {
            Mapper = mapper;
        }

        public override void CustomApply(Snapshot<Guid, TSnapshot> snapshot, IFullyEvent<Guid> fullyEvent)
        {
            switch (fullyEvent.Event)
            {
                case CreatingSnapshotEvent<TSnapshot> evt:
                    CreatingSnapshotHandle(snapshot.State, evt);
                    break;
                default: break;
            }
        }

        private void CreatingSnapshotHandle(TSnapshot snapshotState, CreatingSnapshotEvent<TSnapshot> evt)
        {
            Mapper.Map(evt.Snapshot, snapshotState);
        }

        #region Implementation of ICrudHandle<in TSnapshot>

        public abstract Task Apply(TSnapshot snapshot, IEvent evt);

        #endregion
    }
}