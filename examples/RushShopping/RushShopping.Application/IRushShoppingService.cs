using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RushShopping.Share.Dto;

namespace RushShopping.Application
{
    public interface IRushShoppingService
    {
        Task<Guid> CreateCustomer(CustomerDto dto);
        Task<CustomerDto> GetCustomer(Guid id);
        Task<List<CustomerDto>> GetCustomers();

        Task<Guid> CreateProduct(ProductDto dto);
        Task<ProductDto> GetProduct(Guid id);
        Task<List<ProductDto>> GetProducts();
    }
}