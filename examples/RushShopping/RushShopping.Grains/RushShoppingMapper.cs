using AutoMapper;
using RushShopping.Grains.States;
using RushShopping.Repository.Entities;
using RushShopping.Share.Dto;

namespace RushShopping.Grains
{
    public class RushShoppingMapper
    {
        public static void CreateMapping(IMapperConfigurationExpression configuration)
        {
            configuration.CreateMap<CustomerDto, CustomerState>().ReverseMap();
            configuration.CreateMap<ProductDto, ProductState>().ReverseMap();
            configuration.CreateMap<CustomerDto, Customer>().ReverseMap();
            configuration.CreateMap<ProductDto, Product>().ReverseMap();
            configuration.CreateMap<Customer, CustomerState>().ReverseMap();
            configuration.CreateMap<Product, ProductState>().ReverseMap();
        }
    }
}