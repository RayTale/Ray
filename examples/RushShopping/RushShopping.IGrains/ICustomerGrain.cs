using System;
using System.Threading.Tasks;
using Orleans;

namespace RushShopping.IGrains
{
    public interface ICustomerGrain : IGrainWithGuidKey, ICrudGrain
    {
        Task Create(string name);

        Task AddAmount(decimal amount);

        Task<decimal> GetBalance();

        Task Buy(Guid productId,int quantity);
    }
}