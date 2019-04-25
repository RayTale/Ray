using System;
using System.Threading.Tasks;
using Orleans;

namespace RushShopping.IGrains
{
    public interface ICustomerGrain<TSnapshotDto> : IGrainWithGuidKey, ICrudGrain<TSnapshotDto>
        where TSnapshotDto : class, new()
    {
        Task AddAmount(decimal amount);

        Task<decimal> GetBalance();

        Task Buy(Guid productId, int quantity);
    }
}