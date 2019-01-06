using System.Threading.Tasks;

namespace Ray.Storage.PostgreSQL
{
    public interface IStorageConfig
    {
        Task Configure(IConfigContainer container);
    }
}
