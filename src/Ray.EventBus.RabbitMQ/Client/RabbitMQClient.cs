using Microsoft.Extensions.ObjectPool;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitMQClient : IRabbitMQClient
    {
        readonly ConnectionFactory connectionFactory;
        readonly RabbitOptions options;
        readonly DefaultObjectPool<ModelWrapper> pool;
        public RabbitMQClient(IOptions<RabbitOptions> config)
        {
            options = config.Value;
            connectionFactory = new ConnectionFactory
            {
                UserName = options.UserName,
                Password = options.Password,
                VirtualHost = options.VirtualHost
            };
            pool = new DefaultObjectPool<ModelWrapper>(new ModelPooledObjectPolicy(connectionFactory, options));
        }

        public ModelWrapper PullModel()
        {
            ModelWrapper result;
            bool invalid;
            do
            {
                result = pool.Get();
                if (result.Pool is null)
                    result.Pool = pool;
                if (result.Model.IsClosed || !result.Model.IsOpen)
                {
                    invalid = true;
                    result.ForceDispose();
                }
                else
                {
                    invalid = false;
                }
            } while (invalid);

            return result;
        }
    }
}
