using System;
using Ray.Core;
using RushShopping.Grains.States;
using Orleans;
using Ray.Core.Core.Observer;
using Ray.EventBus.RabbitMQ;

namespace RushShopping.Grains.ProductGrains
{
    [Producer,Observable]
    public class ProductGrain : ConcurrentTxGrain<ProductGrain, Guid, ProductState>
    {
        public ProductGrain()
        {
        }

        public override Guid GrainId => this.GetPrimaryKey();
    }
}
