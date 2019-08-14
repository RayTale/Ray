using System;
using System.Threading.Tasks;

namespace Ray.EventBus.RabbitMQ
{
    public interface IRabbitMQClient : IDisposable
    {
        void PushModel(ModelWrapper model);
        ValueTask<ModelWrapper> PullModel();
    }
}
