using System.Threading.Tasks;
using Ray.Core.Abstractions;
using Ray.Core.EventBus;

namespace Ray.EventBus.RabbitMQ
{
    public interface IRabbitEventBusContainer<W> : IConsumerContainer
        where W : IBytesWrapper
    {
        RabbitEventBus<W> CreateEventBus<K>(string exchange, string queue, int queueCount = 1);
        Task Work(RabbitEventBus<W> bus);
    }
}
