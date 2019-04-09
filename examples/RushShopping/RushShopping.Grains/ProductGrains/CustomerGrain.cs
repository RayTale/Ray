using System;
using Orleans;
using Ray.Core;
using Ray.EventBus.RabbitMQ;
using RushShopping.Grains.States;
using RushShopping.Repository.Entities;

namespace RushShopping.Grains.ProductGrains
{
    [Producer, Observable]
    public class CustomerGrain : RushShoppingGrain<CustomerGrain, Guid, CustomerState, Customer>
    {
        #region Overrides of RayGrain<CustomerGrain,Guid,CustomerState>

        public override Guid GrainId => this.GetPrimaryKey();

        #endregion


    }
}