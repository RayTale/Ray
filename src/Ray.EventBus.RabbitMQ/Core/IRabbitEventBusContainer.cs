using System.Threading.Tasks;
using Ray.Core.EventBus;

namespace Ray.EventBus.RabbitMQ
{
    public interface IRabbitEventBusContainer : IConsumerContainer
    {
        Task AutoRegister();
        RabbitEventBus CreateEventBus(string exchange, string routePrefix, int lBCount = 1, ushort minQos = 100, ushort incQos = 100, ushort maxQos = 300, bool autoAck = false, bool reenqueue = false, bool persistent = false);
        RabbitEventBus CreateEventBus<MainGrain>(string routePrefix, string queue, int lBCount = 1, ushort minQos = 100, ushort incQos = 100, ushort maxQos = 300, bool autoAck = false, bool reenqueue = false, bool persistent = false);
        Task Work(RabbitEventBus bus);
    }
}
