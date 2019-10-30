using Microsoft.Extensions.ObjectPool;
using RabbitMQ.Client;
using System.Collections.Generic;
using System.Threading;

namespace Ray.EventBus.RabbitMQ
{
    public class ModelPooledObjectPolicy : IPooledObjectPolicy<ModelWrapper>
    {
        readonly ConnectionFactory connectionFactory;
        readonly List<ConnectionWrapper> connections = new List<ConnectionWrapper>();
        readonly SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1);
        readonly RabbitOptions options;
        public ModelPooledObjectPolicy(ConnectionFactory connectionFactory, RabbitOptions options)
        {
            this.connectionFactory = connectionFactory;
            this.options = options;
        }
        public ModelWrapper Create()
        {
            foreach (var connection in connections)
            {
                (bool success, ModelWrapper model) = connection.Get();
                if (success)
                    return model;
            }
            semaphoreSlim.Wait();
            try
            {
                if (connections.Count < options.MaxConnection)
                {
                    var connection = new ConnectionWrapper(connectionFactory.CreateConnection(options.EndPoints), options);
                    (bool success, ModelWrapper model) = connection.Get();
                    connections.Add(connection);
                    if (success)
                        return model;
                }
                throw new System.OverflowException(nameof(connections));
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        public bool Return(ModelWrapper obj)
        {
            if (obj.Model.IsOpen)
                return true;
            else
            {
                obj.ForceDispose();
                return false;
            }
        }
    }
}
