using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.ObjectPool;
using RabbitMQ.Client;

namespace Ray.EventBus.RabbitMQ
{
    public class ModelPooledObjectPolicy : IPooledObjectPolicy<ModelWrapper>
    {
        private readonly ConnectionFactory connectionFactory;
        private readonly List<ConnectionWrapper> connections = new List<ConnectionWrapper>();
        private readonly SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1);
        private readonly RabbitOptions options;

        public ModelPooledObjectPolicy(ConnectionFactory connectionFactory, RabbitOptions options)
        {
            this.connectionFactory = connectionFactory;
            this.options = options;
        }

        public ModelWrapper Create()
        {
            foreach (var connection in this.connections)
            {
                (bool success, ModelWrapper model) = connection.Get();
                if (success)
                {
                    return model;
                }
            }

            this.semaphoreSlim.Wait();
            try
            {
                if (this.connections.Count < this.options.MaxConnection)
                {
                    var connection = new ConnectionWrapper(this.connectionFactory.CreateConnection(this.options.EndPoints), this.options);
                    (bool success, ModelWrapper model) = connection.Get();
                    this.connections.Add(connection);
                    if (success)
                    {
                        return model;
                    }
                }

                throw new System.OverflowException(nameof(this.connections));
            }
            finally
            {
                this.semaphoreSlim.Release();
            }
        }

        public bool Return(ModelWrapper obj)
        {
            if (obj.Model.IsOpen)
            {
                return true;
            }
            else
            {
                obj.ForceDispose();
                return false;
            }
        }
    }
}
