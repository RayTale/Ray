using System.Threading.Tasks;

namespace Ray.EventBus.RabbitMQ
{
    public interface IRabbitMQClient : System.IDisposable
    {
        Task ExchangeDeclare(string exchange);
        void PushModel(ModelWrapper model);
        ValueTask<ModelWrapper> PullModel();
    }
}
