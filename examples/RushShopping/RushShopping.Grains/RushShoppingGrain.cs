using System;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core;
using Ray.Core.Snapshot;
using RushShopping.Grains.Events;
using RushShopping.IGrains;
using RushShopping.Repository;

namespace RushShopping.Grains
{
    public abstract class RushShoppingGrain<TPrimaryKey, TSnapshotType, TEntityType, TSnapshotDto> :
        ConcurrentTxGrain<TPrimaryKey, TSnapshotType>
        , ICrudGrain<TSnapshotDto>
        where TSnapshotType : class, ICloneable<TSnapshotType>, TEntityType, new()
        where TEntityType : class, IEntity<TPrimaryKey>
        where TSnapshotDto : class, new()
    {
        protected IMapper Mapper { get; private set; }

        protected override async ValueTask CreateSnapshot()
        {
            using (var repository = ServiceProvider.GetService<IGrainRepository<TEntityType, TPrimaryKey>>())
            {
                var entity = await repository.FirstOrDefaultAsync(GrainId);
                if (entity != null)
                {
                    Snapshot = new Snapshot<TPrimaryKey, TSnapshotType>(GrainId)
                    {
                        State = Mapper.Map<TSnapshotType>(entity)
                    };
                }
                else
                {
                    await base.CreateSnapshot();
                }
            }
        }

        protected override ValueTask DependencyInjection()
        {
            Mapper = ServiceProvider.GetService<IMapper>();
            return base.DependencyInjection();
        }

        #region Implementation of ICrudGrain<TSnapshotDto>

        public Task Create(TSnapshotDto snapshotDto)
        {
            var snapshotState = Mapper.Map<TSnapshotType>(snapshotDto);
            var evt = new CreatingSnapshotEvent<TSnapshotType>(snapshotState);
            return RaiseEvent(evt);
        }

        public Task<TSnapshotDto> Get()
        {
            return Task.FromResult(Mapper.Map<TSnapshotDto>(Snapshot.State));
        }

        public Task Update(TSnapshotDto snapshot)
        {
            throw new NotImplementedException();
        }

        public Task Delete()
        {
            throw new NotImplementedException();
        }

        #endregion
    }

    public abstract class
        RushShoppingGrain<TPrimaryKey, TStateType, TSnapshotDto> : RushShoppingGrain<TPrimaryKey, TStateType, TStateType
            , TSnapshotDto>
        where TStateType : class, ICloneable<TStateType>, IEntity<TPrimaryKey>, new()
        where TSnapshotDto : class, new()
    {

    }
}