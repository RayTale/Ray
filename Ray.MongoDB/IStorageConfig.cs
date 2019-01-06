using System.Threading.Tasks;

namespace Ray.Storage.MongoDB
{
    public interface IStorageConfig
    {
        Task Configure(IConfigContainer container);
    }
}
