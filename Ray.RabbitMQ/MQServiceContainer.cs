using System;
using System.Collections.Concurrent;
using Ray.Core;
using Ray.Core.MQ;

namespace Ray.RabbitMQ
{
    public class MQServiceContainer<W> : IMQServiceContainer
        where W : MessageWrapper, new()
    {
        ConcurrentDictionary<Type, IMQService> typeMQDict = new ConcurrentDictionary<Type, IMQService>();
        public IMQService GetService(Type type)
        {
            if (!typeMQDict.TryGetValue(type, out var value))
            {
                var mqPubAttr = GetRabbitMQAttr(type);
                value = new RabbitMQService<W>(mqPubAttr);
                typeMQDict.TryAdd(type, value);
            }
            return value;
        }
        static Type rabbitMQType = typeof(RabbitPubAttribute);
        public static RabbitPubAttribute GetRabbitMQAttr(Type type)
        {
            var rabbitMQAttributes = type.GetCustomAttributes(rabbitMQType, true);
            RabbitPubAttribute mqAttr = null;
            if (rabbitMQAttributes.Length > 0)
            {
                mqAttr = rabbitMQAttributes[0] as RabbitPubAttribute;
                if (string.IsNullOrEmpty(mqAttr.Exchange))
                {
                    mqAttr.Exchange = type.Namespace;
                }
                if (string.IsNullOrEmpty(mqAttr.Queue))
                {
                    mqAttr.Queue = type.Name;
                }
            }
            if (mqAttr == null)
            {
                mqAttr = new RabbitPubAttribute(type.Namespace, type.Name);
            }
            mqAttr.Init();
            return mqAttr;
        }
    }
}
