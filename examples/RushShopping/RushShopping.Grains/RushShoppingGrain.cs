using System;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.Extensions.DependencyInjection;
using Ray.Core;
using Ray.Core.Snapshot;
using RushShopping.IGrains;
using RushShopping.Repository;

namespace RushShopping.Grains
{
    public abstract class RushShoppingGrain<TGrain, TPrimaryKey, TSnapshotType, TEntityType,TSnapshotDto> : ConcurrentTxGrain<TGrain, TPrimaryKey, TSnapshotType>
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

        public Task Create(TSnapshotDto snapshot)
        {
            var snapshot = Mapper.Map<TSnapshotType>(snapshot);
            var evt =new CreatingSnapshotEvent(snapshot);
        }

        public Task<TSnapshotDto> Get()
        {
            throw new NotImplementedException();
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

    public abstract class RushShoppingGrain<TGrain, TPrimaryKey, TStateType> : RushShoppingGrain<TGrain, TPrimaryKey, TStateType, TStateType>
        where TStateType : class, ICloneable<TStateType>, IEntity<TPrimaryKey>, new()
    {

    }
}