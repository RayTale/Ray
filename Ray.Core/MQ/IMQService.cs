using System.Threading.Tasks;

namespace Ray.Core.MQ
{
    public interface IMQService
    {
        ValueTask Publish(byte[] bytes, string hashKey);
    }
}
