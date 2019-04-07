using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public interface IProducerContainer
    {
        ValueTask<IProducer> GetProducer<T>(T data);
    }
}
