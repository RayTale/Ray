using Ray.Core.MQ;
using Ray.Grain;
using Ray.RabbitMQ;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Ray.Grain
{
    public class MQServiceContainer : IMQServiceContainer
    {
        readonly IRabbitMQClient rabbitMQClient;
        public MQServiceContainer(IRabbitMQClient rabbitMQClient)
        {
            this.rabbitMQClient = rabbitMQClient;
            PublisherDictionary = new Dictionary<Type, RabbitPublisher>
            {
                { typeof(Account),new RabbitPublisher("Account", "account", 20)}
            };
        }
        public Dictionary<Type, RabbitPublisher> PublisherDictionary { get; }
        public ConcurrentDictionary<Type, IMQService> ServiceDictionary = new ConcurrentDictionary<Type, IMQService>();
        public async ValueTask<IMQService> GetService(Orleans.Grain grain)
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
                    throw new NotImplementedException(nameof(GetService));
                }
            }
            return service;
        }
    }
}
