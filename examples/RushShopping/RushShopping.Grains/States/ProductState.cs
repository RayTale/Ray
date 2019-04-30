using System;
using RushShopping.Repository.Entities;
using Ray.Core.Snapshot;

namespace RushShopping.Grains.States
{
    [Serializable]
    public class ProductState : Product, ICloneable<ProductState>
    {
        public ProductState Clone() => new ProductState()
        {
            Id = Id,
            Name = Name,
            Price = Price,
            RemainsCount = RemainsCount,
            TotalCount = TotalCount
        };
    }
}