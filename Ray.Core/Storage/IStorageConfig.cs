using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public interface IStorageConfig
    {
        bool Singleton { get; set; }
        ValueTask Init();
    }
}
