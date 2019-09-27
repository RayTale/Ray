using Orleans;
using Orleans.Concurrency;
using System.Threading.Tasks;

namespace Ray.Core.Services
{
    public interface IUtcUID : IGrainWithStringKey
    {
        /// <summary>
        /// 通过utc时间生成分布式唯一id
        /// </summary>
        [AlwaysInterleave]
        Task<string> NewID();
    }
}
