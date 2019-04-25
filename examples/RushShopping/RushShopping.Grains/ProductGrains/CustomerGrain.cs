using System;
using System.Threading.Tasks;
using Orleans;
using Ray.Core;
using Ray.EventBus.RabbitMQ;
using RushShopping.Grains.States;
using RushShopping.IGrains;
using RushShopping.Repository.Entities;
using RushShopping.Share.Dto;

namespace RushShopping.Grains.ProductGrains
{
    [Producer(lBCount: 4), Observable]
    public class CustomerGrain : RushShoppingGrain<CustomerGrain, Guid, CustomerState,Customer, CustomerDto>, ICustomerGrain<CustomerDto>
    {
        #region Overrides of RayGrain<CustomerGrain,Guid,CustomerState>

        public override Guid GrainId => this.GetPrimaryKey();

        #endregion

        #region Implementation of ICustomerGrain<CustomerDto>

        public Task AddAmount(decimal amount)
        {
            throw new NotImplementedException();
        }

        public Task<decimal> GetBalance()
        {
            throw new NotImplementedException();
        }

        public Task Buy(Guid productId, int quantity)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}