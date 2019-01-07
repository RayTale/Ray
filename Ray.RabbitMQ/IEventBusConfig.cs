using System.Threading.Tasks;
using Ray.Core.Serialization;

namespace Ray.EventBus.RabbitMQ
{
    public interface IEventBusConfig<W>
        where W : IBytesWrapper
    {
        Task Configure(IRabbitEventBusContainer<W> busContainer);
    }
}
