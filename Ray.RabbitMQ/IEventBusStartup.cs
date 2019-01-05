using System.Threading.Tasks;
using Ray.Core.Abstractions;

namespace Ray.EventBus.RabbitMQ
{
    public interface IEventBusStartup<W>
        where W : IBytesWrapper
    {
        Task ConfigureEventBus(IRabbitEventBusContainer<W> busContainer);
    }
}
