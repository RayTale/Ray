using System;
using Ray.Core;
using RushShopping.Grains.States;
using Orleans;
namespace RushShopping.Grains.ProductGrains
{
    public class ProductGrain : ConcurrentTxGrain<ProductGrain, Guid, ProductState>
    {
        public ProductGrain()
        {
        }

        public override Guid GrainId => this.GetPrimaryKey();
    }
}
