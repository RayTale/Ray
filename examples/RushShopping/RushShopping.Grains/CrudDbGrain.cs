using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ray.Core;
using Ray.Core.Event;
using RushShopping.Grains.Events;
using RushShopping.IGrains;
using RushShopping.Repository;

namespace RushShopping.Grains
{
    public abstract class CrudDbGrain<TMain, TSnapshot, TPrimaryKey, TEntityType> : ConcurrentObserverGrain<TMain, TPrimaryKey>, ICrudDbGrain<TPrimaryKey>
        where TSnapshot : class, new() where TEntityType : class, IEntity<TPrimaryKey>
    {
        protected ICrudHandle<TPrimaryKey,TSnapshot> CrudHandle;
        protected IMapper Mapper;
        protected CrudDbGrain(ILogger logger) : base(logger)
        {
        }


        protected override ValueTask DependencyInjection()
        {
            CrudHandle = ServiceProvider.GetService<ICrudHandle<TPrimaryKey,TSnapshot >> ();
            Mapper = ServiceProvider.GetService<IMapper>();
            return base.DependencyInjection();
        }

        #region Overrides of ObserverGrain<TMain,TPrimaryKey>

        protected override async ValueTask OnEventDelivered(IFullyEvent<TPrimaryKey> @event)
        {
            switch (@event.Event)
            {
                case CreatingSnapshotEvent<TSnapshot> evt:
                   await CreatingSnapshotHandle(evt);
                    break;
            }
            await Process(@event);
        }

        private async Task CreatingSnapshotHandle(CreatingSnapshotEvent<TSnapshot> evt)
        {
            using (var repository = ServiceProvider.GetService<IGrainRepository<TEntityType, TPrimaryKey>>())
            {
                var entity = Mapper.Map<TEntityType>(evt.Snapshot);
                await repository.InsertAsync(entity);
                await repository.CommitAsync();
            }
        }

        #endregion

        public abstract Task Process(IFullyEvent<TPrimaryKey> @event);
    }
}