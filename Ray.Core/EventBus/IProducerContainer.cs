using Orleans;
using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public interface IProducerContainer
    {
        ValueTask<IProducer> GetProducer(Grain grain);
    }
}
