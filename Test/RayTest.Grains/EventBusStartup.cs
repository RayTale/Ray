using System.Threading.Tasks;
using Ray.EventBus.RabbitMQ;
using RayTest.IGrains;

namespace RayTest.Grains
{
    public class EventBusStartup : IEventBusStartup<MessageInfo>
    {
        public Task ConfigureEventBus(IRabbitEventBusContainer<MessageInfo> busContainer)
        {
            return busContainer.CreateEventBus<long>("Account", "account", 5).BindProducer<Account>().Enable();
        }
    }
}
