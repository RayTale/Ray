using System;
using Microsoft.Extensions.ObjectPool;
using RabbitMQ.Client;

namespace Ray.EventBus.RabbitMQ
{
    public class ModelWrapper : IDisposable
    {
        private readonly IBasicProperties persistentProperties;
        private readonly IBasicProperties noPersistentProperties;

        public DefaultObjectPool<ModelWrapper> Pool { get; set; }

        public ConnectionWrapper Connection { get; set; }

        public IModel Model { get; set; }

        public ModelWrapper(
            ConnectionWrapper connectionWrapper,
            IModel model)
        {
            this.Connection = connectionWrapper;
            this.Model = model;
            this.persistentProperties = this.Model.CreateBasicProperties();
            this.persistentProperties.Persistent = true;
            this.noPersistentProperties = this.Model.CreateBasicProperties();
            this.noPersistentProperties.Persistent = false;
        }

        public void Publish(byte[] msg, string exchange, string routingKey, bool persistent = true)
        {
            this.Model.BasicPublish(exchange, routingKey, persistent ? this.persistentProperties : this.noPersistentProperties, msg);
        }

        public void Dispose()
        {
            this.Pool.Return(this);
        }

        public void ForceDispose()
        {
            this.Model.Close();
            this.Model.Dispose();
            this.Connection.Return(this);
        }
    }
}
