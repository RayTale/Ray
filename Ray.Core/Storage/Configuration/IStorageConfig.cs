using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public interface IStorageConfig
    {
        /// <summary>
        /// 是否是单实例
        /// </summary>
        bool Singleton { get; set; }
        ValueTask Init();
    }
}
