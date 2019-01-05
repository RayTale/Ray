using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.Abstractions;
using Ray.Core.EventBus;

namespace Ray.EventBus.RabbitMQ
{
    public interface IRabbitEventBusContainer<W> where W : IBytesWrapper
    {
        RabbitEventBus<W> CreateEventBus<K>(string exchange, string queue, int queueCount = 1);
        List<IConsumer> GetConsumers();
        Task Work(RabbitEventBus<W> bus);
    }
}
