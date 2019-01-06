using System.Threading.Tasks;
using Ray.EventBus.RabbitMQ;
using RayTest.IGrains;

namespace RayTest.Grains
{
    public class EventBusStartup : IEventBusConfig<MessageInfo>
    {
        public Task Configure(IRabbitEventBusContainer<MessageInfo> busContainer)
        {
            return busContainer.CreateEventBus<long>("Account", "account", 5).BindProducer<Account>().Enable();
        }
    }
}
