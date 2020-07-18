using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace Ray.Core.Services
{
    public interface ILocalUID : IGrainWithStringKey
    {
        /// <summary>
        /// 通过utc时间生成分布式唯一id
        /// </summary>
        [AlwaysInterleave]
        Task<string> NewID();
    }
}
