using System.Threading.Tasks;
using Ray.Core.EventBus;

namespace Ray.EventBus.RabbitMQ
{
    public interface IRabbitEventBusContainer : IConsumerContainer
    {
        RabbitEventBus CreateEventBus(string exchange, string queue, int queueCount = 1);
        RabbitEventBus CreateEventBus<MainGrain>(string exchange, string queue, int queueCount = 1);
        Task Work(RabbitEventBus bus);
    }
}
