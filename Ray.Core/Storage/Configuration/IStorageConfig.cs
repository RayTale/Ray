using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public interface IStorageConfig
    {
        /// <summary>
        /// 是否是单实例
        /// </summary>
        bool Singleton { get; set; }
        /// <summary>
        /// 唯一名称，一般可以使用Grain的名称
        /// </summary>
        string UniqueName { get; set; }
        ValueTask Build();
    }
}
