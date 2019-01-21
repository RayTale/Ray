using System;
using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public interface IProducer
    {
        Type GrainType { get; }
        ValueTask Publish(byte[] bytes, string hashKey);
    }
}
