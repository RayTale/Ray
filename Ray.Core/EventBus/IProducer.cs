using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public interface IProducer
    {
        ValueTask Publish(byte[] bytes, string hashKey);
    }
}
