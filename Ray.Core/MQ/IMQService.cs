using Ray.Core.Message;
using System.Threading.Tasks;

namespace Ray.Core.MQ
{
    public interface IMQService
    {
        Task Publish(IMessage msg, byte[] bytes, string hashKey);
    }
}
