using Ray.Core.EventBus;
using Ray.Grain;
using Ray.EventBus.RabbitMQ;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Ray.Grain
{
    public class ProducerContainer : IProducerContainer
    {
        readonly IRabbitMQClient rabbitMQClient;
        public ProducerContainer(IRabbitMQClient rabbitMQClient)
        {
            this.rabbitMQClient = rabbitMQClient;
            PublisherDictionary = new Dictionary<Type, RabbitPublisher>
            {
                { typeof(Account),new RabbitPublisher("Account", "account", 20)}
            };
        }
        public Dictionary<Type, RabbitPublisher> PublisherDictionary { get; }
        public ConcurrentDictionary<Type, IProducer> ServiceDictionary = new ConcurrentDictionary<Type, IProducer>();
        public async ValueTask<IProducer> GetProducer(Orleans.Grain grain)
        {
            var type = grain.GetType();
            if (!ServiceDictionary.TryGetValue(type, out var service))
            {
                if (PublisherDictionary.TryGetValue(type, out var value))
                {
                    var buildTask = value.Build(rabbitMQClient);
                    if (!buildTask.IsCompleted)
                        await buildTask;
                    service = ServiceDictionary.GetOrAdd(type, key =>
                     {
                         return new RabbitMQService(value);
                     });
                }
                else
                {
                    throw new NotImplementedException(nameof(GetProducer));
                }
            }
            return service;
        }
    }
}
