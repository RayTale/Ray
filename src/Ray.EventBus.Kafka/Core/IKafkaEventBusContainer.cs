using System.Threading.Tasks;
using Ray.Core.EventBus;

namespace Ray.EventBus.Kafka
{
    public interface IKafkaEventBusContainer : IConsumerContainer
    {
        Task AutoRegister();

        KafkaEventBus CreateEventBus(string topic, int lBCount = 1, int retryCount = 3, int retryIntervals = 500);

        KafkaEventBus CreateEventBus<MainGrain>(string topic, int lBCount = 1, int retryCount = 3, int retryIntervals = 500);

        Task Work(KafkaEventBus bus);
    }
}
