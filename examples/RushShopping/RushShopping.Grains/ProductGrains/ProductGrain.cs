using System;
using System.Threading.Tasks;
using Ray.Core;
using RushShopping.Grains.States;
using Orleans;
using Ray.EventBus.RabbitMQ;
using RushShopping.IGrains;
using RushShopping.Repository.Entities;
using RushShopping.Share.Dto;

namespace RushShopping.Grains.ProductGrains
{
    [Producer(lBCount: 4), Observable]
    public class ProductGrain : RushShoppingGrain<Guid, ProductState, Product, ProductDto>, IProductGrain<ProductDto>
    {
        public override Guid GrainId => this.GetPrimaryKey();

        #region Implementation of IProductGrain<ProductDto>

        public Task<int> GetResidualQuantity()
        {
            throw new NotImplementedException();
        }

        public Task SellOut(int quantity, decimal unitPrice)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
