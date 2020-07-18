using System.Threading.Tasks;
using Ray.Core.EventBus;

namespace Ray.EventBus.RabbitMQ
{
    public interface IRabbitEventBusContainer : IConsumerContainer
    {
        Task AutoRegister();

        RabbitEventBus CreateEventBus(string exchange, string routePrefix, int lBCount = 1, bool autoAck = false, bool persistent = false, int retryCount = 3, int retryIntervals = 500);

        RabbitEventBus CreateEventBus<MainGrain>(string routePrefix, string queue, int lBCount = 1, bool autoAck = false, bool persistent = false, int retryCount = 3, int retryIntervals = 500);

        Task Work(RabbitEventBus bus);
    }
}
