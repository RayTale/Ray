using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using RushShopping.IGrains;
using RushShopping.Share.Dto;

namespace RushShopping.Application
{
    public class RushShoppingService : IRushShoppingService
    {
        protected IClusterClient ClusterClient;

        public RushShoppingService(IClusterClient clusterClient)
        {
            ClusterClient = clusterClient;
        }

        #region Implementation of IRushShoppingService

        public async Task<Guid> CreateCustomer(CustomerDto dto)
        {
            var grainId = Guid.NewGuid();
            var customerGrain = ClusterClient.GetGrain<ICustomerGrain<CustomerDto>>(grainId);
            await customerGrain.Create(dto);
            return grainId;
        }

        public Task<CustomerDto> GetCustomer(Guid id)
        {
            throw new NotImplementedException();
        }

        public Task<List<CustomerDto>> GetCustomers()
        {
            throw new NotImplementedException();
        }

        public Task<Guid> CreateProduct(ProductDto dto)
        {
            throw new NotImplementedException();
        }

        public Task<ProductDto> GetProduct(Guid id)
        {
            throw new NotImplementedException();
        }

        public Task<List<ProductDto>> GetProducts()
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}