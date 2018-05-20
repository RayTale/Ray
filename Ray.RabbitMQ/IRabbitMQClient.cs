using System.Threading.Tasks;

namespace Ray.RabbitMQ
{
    public interface IRabbitMQClient : System.IDisposable
    {
        Task ExchangeDeclare(string exchange);
        void PushModel(ModelWrapper model);
        Task<ModelWrapper> PullModel();
    }
}
