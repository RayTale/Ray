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
            if (dto.Id == default(Guid))
            {
                dto.Id = Guid.NewGuid();
            }
            var customerGrain = ClusterClient.GetGrain<ICustomerGrain<CustomerDto>>(dto.Id);
            await customerGrain.Create(dto);
            return dto.Id;
        }

        public Task<CustomerDto> GetCustomer(Guid id)
        {
            return ClusterClient.GetGrain<ICustomerGrain<CustomerDto>>(id).Get();
        }

        public Task UpdateCustomer(CustomerDto dto)
        {
            return ClusterClient.GetGrain<ICustomerGrain<CustomerDto>>(dto.Id).Update(dto);
        }

        public async Task DeleteCustomer(Guid id)
        {
            await ClusterClient.GetGrain<ICustomerGrain<CustomerDto>>(id).Delete();
            await Task.Delay(500);
            await ClusterClient.GetGrain<ICustomerGrain<CustomerDto>>(id).Over();
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