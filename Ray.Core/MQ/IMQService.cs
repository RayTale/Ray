using System.Threading.Tasks;

namespace Ray.Core.MQ
{
    public interface IMQService
    {
        void Publish(byte[] bytes, string hashKey);
    }
}
