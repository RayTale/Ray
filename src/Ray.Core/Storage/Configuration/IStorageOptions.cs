using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public interface IStorageOptions
    {
        /// <summary>
        /// 是否是单实例
        /// </summary>
        bool Singleton { get; set; }

        /// <summary>
        /// 唯一名称，一般可以使用Grain的名称
        /// <remarks>Grain名称在不同名称空间下，可以同名，建议更名为GrainName</remarks>
        /// </summary>
        string UniqueName { get; set; }

        ValueTask Build();
    }
}