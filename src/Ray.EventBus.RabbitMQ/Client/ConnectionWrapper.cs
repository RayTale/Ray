using System.Collections.Generic;
using System.Threading;
using RabbitMQ.Client;

namespace Ray.EventBus.RabbitMQ
{
    public class ConnectionWrapper
    {
        private readonly List<ModelWrapper> models = new List<ModelWrapper>();
        private readonly IConnection connection;
        private readonly SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1);

        public ConnectionWrapper(
            IConnection connection,
           RabbitOptions options)
        {
            this.connection = connection;
            this.Options = options;
        }

        public RabbitOptions Options { get; }

        public (bool success, ModelWrapper model) Get()
        {
            this.semaphoreSlim.Wait();
            try
            {
                if (this.models.Count < this.Options.PoolSizePerConnection)
                {
                    var model = new ModelWrapper(this, this.connection.CreateModel());
                    this.models.Add(model);
                    return (true, model);
                }
            }
            finally
            {
                this.semaphoreSlim.Release();
            }

            return (false, default);
        }

        public void Return(ModelWrapper model)
        {
            this.models.Remove(model);
        }
    }
}
