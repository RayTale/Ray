using System;
using System.Collections.Concurrent;
using Orleans;
using Ray.Core;
using Ray.Core.MQ;

namespace Ray.RabbitMQ
{
    public class MQServiceContainer<W> : IMQServiceContainer
        where W : IMessageWrapper, new()
    {
        IRabbitMQClient client;
        public MQServiceContainer(IRabbitMQClient client)
        {
            this.client = client;
        }
        ConcurrentDictionary<Type, IMQService> serviceDict = new ConcurrentDictionary<Type, IMQService>();
        object typeLock = new object();
        public IMQService GetService(Type type, Grain grain)
        {
            if (!serviceDict.TryGetValue(type, out var value))
            {
                lock (typeLock)
                {
                    value = new RabbitMQService(GetAttribute(type));
                    serviceDict.TryAdd(type, value);
                }
            }
            return value;
        }
        static Type rabbitMQType = typeof(RabbitPubAttribute);
        public RabbitPubAttribute GetAttribute(Type type)
        {
            var rabbitMQAttributes = type.GetCustomAttributes(rabbitMQType, true);
            RabbitPubAttribute pubAttribute = null;
            if (rabbitMQAttributes.Length > 0)
            {
                pubAttribute = rabbitMQAttributes[0] as RabbitPubAttribute;
                if (string.IsNullOrEmpty(pubAttribute.Exchange))
                {
                    pubAttribute.Exchange = type.Namespace;
                }
                if (string.IsNullOrEmpty(pubAttribute.Queue))
                {
                    pubAttribute.Queue = type.Name;
                }
            }
            if (pubAttribute == null)
            {
                pubAttribute = new RabbitPubAttribute(type.Namespace, type.Name);
            }
            pubAttribute.Init(client);
            return pubAttribute;
        }
    }
}
