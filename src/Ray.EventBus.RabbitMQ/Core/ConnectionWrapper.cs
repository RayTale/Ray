using RabbitMQ.Client;
using System.Collections.Generic;
using System.Threading;

namespace Ray.EventBus.RabbitMQ
{
    public class ConnectionWrapper
    {
        private readonly List<ModelWrapper> models = new List<ModelWrapper>();
        private readonly IConnection connection;
        readonly int poolSize;
        readonly SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1);
        public ConnectionWrapper(
            IConnection connection,
            int poolSize)
        {
            this.connection = connection;
            this.poolSize = poolSize;
        }
        public (bool success, ModelWrapper model) Get()
        {
            semaphoreSlim.Wait();
            try
            {
                if (models.Count < poolSize)
                {
                    var model = new ModelWrapper(this, connection.CreateModel());
                    models.Add(model);
                    return (true, model);
                }
            }
            finally
            {
                semaphoreSlim.Release();
            }
            return (false, default);
        }
        public void Return(ModelWrapper model)
        {
            models.Remove(model);
        }
    }
}
