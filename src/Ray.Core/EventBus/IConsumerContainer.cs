using System.Collections.Generic;

namespace Ray.Core.EventBus
{
    public interface IConsumerContainer
    {
        List<IConsumer> GetConsumers();
    }
}
