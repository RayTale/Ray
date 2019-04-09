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
    public abstract class RushShoppingGrain<TGrain, TPrimaryKey, TSnapshotType, TEntityType> : ConcurrentTxGrain<TGrain, TPrimaryKey, TSnapshotType>
    
        where TSnapshotType : class, ICloneable<TSnapshotType>, TEntityType, new()
        where TEntityType : class, IEntity<TPrimaryKey>
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

        #region Implementation of ICrudGrain

        public Task Create(TSnapshotType snapshot)
        {
            throw new NotImplementedException();
        }

        public Task<TSnapshotType> Get()
        {
            throw new NotImplementedException();
        }

        public Task Update(TSnapshotType snapshotType)
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