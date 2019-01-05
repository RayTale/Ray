using System.Threading.Tasks;
using Ray.EventBus.RabbitMQ;
using Ray.IGrains;
using Ray.IGrains.Actors;

namespace Ray.Grain
{
    public class EventBusStartup : IEventBusStartup<MessageInfo>
    {
        public Task ConfigureEventBus(IRabbitEventBusContainer<MessageInfo> busContainer)
        {
            var eventBus = busContainer.CreateEventBus<long>("Account", "account", 20).BindProducer<Account>();
            eventBus.DefiningConsumer<long>(DefaultPrefix.primary).BindConcurrentFollowWithLongId<IAccountFlow>().BindFollowWithLongId<IAccountRep>();
            eventBus.DefiningConsumer<long>(DefaultPrefix.secondary).BindConcurrentFollowWithLongId<IAccountDb>();
            return eventBus.Complete();
        }
    }
}
