using System;
using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public interface IProducerContainer
    {
        ValueTask<IProducer> GetProducer<T>();
        ValueTask<IProducer> GetProducer(Type type);
    }
}
