using Ray.Core.EventBus;
using System.Threading.Tasks;

namespace Ray.EventBus.Kafka
{
    public interface IKafkaEventBusContainer : IConsumerContainer
    {
        Task AutoRegister();
        KafkaEventBus CreateEventBus(string topic, int lBCount = 1, bool reenqueue = true);
        KafkaEventBus CreateEventBus<MainGrain>(string topic, int lBCount = 1, bool reenqueue = true);
        Task Work(KafkaEventBus bus);
    }
}
