using System.Threading.Tasks;
using Orleans;

namespace RushShopping.IGrains
{
    public interface IProductGrain<TSnapshotDto> : IGrainWithGuidKey, ICrudGrain<TSnapshotDto>
        where TSnapshotDto : class, new()
    {
        /// <summary>
        /// 获取剩余的商品数量
        /// </summary>
        /// <returns></returns>
        Task<int> GetResidualQuantity();

        Task SellOut(int quantity, decimal unitPrice);
    }
}