using Ray.Core.EventBus;
using System.Threading.Tasks;

namespace Ray.EventBus.Kafka
{
    public interface IKafkaEventBusContainer : IConsumerContainer
    {
        Task AutoRegister();
        KafkaEventBus CreateEventBus(string topic, int lBCount = 1);
        KafkaEventBus CreateEventBus<MainGrain>(string topic, int lBCount = 1);
        Task Work(KafkaEventBus bus);
    }
}
