using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public interface IStorageConfig
    {
        bool Singleton { get; set; }
        ValueTask Init();
    }
    public interface IStorageConfigParameter
    {
        bool Singleton { get; set; }
    }
}
