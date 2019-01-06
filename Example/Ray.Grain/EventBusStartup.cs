using System.Threading.Tasks;
using Ray.EventBus.RabbitMQ;
using Ray.IGrains;
using Ray.IGrains.Actors;

namespace Ray.Grain
{
    public class EventBusStartup : IEventBusStartup<MessageInfo>
    {
        public async Task ConfigureEventBus(IRabbitEventBusContainer<MessageInfo> busContainer)
        {
            await busContainer.CreateEventBus<long>("Account", "account", 5).BindProducer<Account>().
                      CreateConsumer<long>(DefaultPrefix.primary).BindConcurrentFollowWithLongId<IAccountFlow>().BindFollowWithLongId<IAccountRep>().Complete().
                      CreateConsumer<long>(DefaultPrefix.secondary).BindConcurrentFollowWithLongId<IAccountDb>().Complete()
                  .Enable();
        }
    }
}
