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
                VirtualHost = options.VirtualHost,
                AutomaticRecoveryEnabled = false
            };
            pool = new DefaultObjectPool<ModelWrapper>(new ModelPooledObjectPolicy(connectionFactory, options));
        }

        public ModelWrapper PullModel()
        {
            var result = pool.Get();
            if (result.Pool is null)
                result.Pool = pool;
            return result;
        }
    }
}
