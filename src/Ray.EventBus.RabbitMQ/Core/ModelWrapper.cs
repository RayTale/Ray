using Microsoft.Extensions.ObjectPool;
using RabbitMQ.Client;
using System;

namespace Ray.EventBus.RabbitMQ
{
    public class ModelWrapper : IDisposable
    {
        readonly IBasicProperties persistentProperties;
        readonly IBasicProperties noPersistentProperties;
        public DefaultObjectPool<ModelWrapper> Pool { get; set; }
        public ConnectionWrapper Connection { get; set; }
        public IModel Model { get; set; }
        public ModelWrapper(
            ConnectionWrapper connectionWrapper,
            IModel model)
        {
            Connection = connectionWrapper;
            Model = model;
            persistentProperties = Model.CreateBasicProperties();
            persistentProperties.Persistent = true;
            noPersistentProperties = Model.CreateBasicProperties();
            noPersistentProperties.Persistent = false;
        }
        public void Publish(byte[] msg, string exchange, string routingKey, bool persistent = true)
        {
            Model.BasicPublish(exchange, routingKey, persistent ? persistentProperties : noPersistentProperties, msg);
        }
        public void Dispose()
        {
            Pool.Return(this);
        }
        public void ForceDispose()
        {
            Model.Close();
            Model.Dispose();
            Connection.Return(this);
        }
    }
}
