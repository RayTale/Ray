using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace Ray.Core.IGrains
{
    public interface IUID : IGrainWithStringKey
    {
        /// <summary>
        /// 通过utc时间生成分布式唯一id
        /// </summary>
        /// <returns>19位数字字符，支持转换为Int64类型</returns>
        [AlwaysInterleave]
        Task<string> NewUtcID();
        /// <summary>
        /// 通过本地时间生成分布式唯一id
        /// </summary>
        /// <returns>19位数字字符，支持转换为Int64类型</returns>
        [AlwaysInterleave]
        Task<string> NewLocalID();
    }
}
