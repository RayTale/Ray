using System;
using System.Threading.Tasks;

namespace Ray.RabbitMQ
{
    public interface IRabbitMQClient
    {
        Task ExchangeDeclare(string exchange);
        Task Publish(byte[] msg, string exchange, string queue, bool persistent = true);
        Task PublishByCmd<T>(UInt16 cmd, T data, string exchange, string queue, bool persistent = false);
        Task Publish<T>(T data, string exchange, string queue, bool persistent = true);
        void PushModel(ModelWrapper model);
        Task<ModelWrapper> PullModel();
    }
}
