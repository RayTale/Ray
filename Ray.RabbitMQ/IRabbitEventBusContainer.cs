using System.Threading.Tasks;
using Ray.Core.EventBus;
using Ray.Core.Serialization;

namespace Ray.EventBus.RabbitMQ
{
    public interface IRabbitEventBusContainer<W> : IConsumerContainer
        where W : IBytesWrapper
    {
        RabbitEventBus<W> CreateEventBus<K>(string exchange, string queue, int queueCount = 1);
        Task Work(RabbitEventBus<W> bus);
    }
}
