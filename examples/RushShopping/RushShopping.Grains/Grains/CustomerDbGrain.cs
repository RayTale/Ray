using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core;
using Ray.Core.Event;
using RushShopping.Grains.States;
using RushShopping.IGrains;
using RushShopping.Repository.Entities;
using RushShopping.Share.Dto;

namespace RushShopping.Grains.Grains
{
    [Observer(DefaultObserverGroup.primary, "db", typeof(CustomerGrain))]
    public class CustomerDbGrain : CrudDbGrain<CustomerGrain, CustomerState,Guid, Customer>, ICustomerDbGrain
    {
        #region Overrides of CrudDbGrain<ProductGrain,ProductState,Guid,Product>

        public override Task Process(IFullyEvent<Guid> @event)
        {
            return Task.CompletedTask;
        }

        #endregion

        public CustomerDbGrain(IGrainFactory grainFactory) : base(grainFactory)
        {
        }
    }
}