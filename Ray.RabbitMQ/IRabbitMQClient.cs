using System.Threading.Tasks;

namespace Ray.EventBus.RabbitMQ
{
    public interface IRabbitMQClient : System.IDisposable
    {
        void PushModel(ModelWrapper model);
        ValueTask<ModelWrapper> PullModel();
    }
}
