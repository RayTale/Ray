using Ray.Core.Message;
using System.Threading.Tasks;

namespace Ray.Core.MQ
{
    public interface IMQService
    {
        Task Send(IMessage msg, byte[] bytes, string hashKey);
    }
}
