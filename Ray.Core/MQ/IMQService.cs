using System.Threading.Tasks;

namespace Ray.Core.MQ
{
    public interface IMQService
    {
        Task Publish(byte[] bytes, string hashKey);
    }
}
