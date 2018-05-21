using System;
using System.Collections.Generic;
using Ray.Core.Utils;
using System.Linq;

namespace Ray.RabbitMQ
{
    [AttributeUsage(AttributeTargets.Class)]
    public class RabbitPubAttribute : Attribute
    {
        public RabbitPubAttribute(string exchange = null, string queue = null, int queueCount = 1)
        {
            Exchange = exchange;
            Queue = queue;
            QueueCount = queueCount;
        }
        ConsistentHash _CHash;
        public IRabbitMQClient Client { get; set; }
        List<string> nodeList;
        Dictionary<string, ModelWrapper> models = new Dictionary<string, ModelWrapper>();
        public void Init(IRabbitMQClient client)
        {
            var isOnce = QueueCount == 1;
            if (isOnce)
            {
                models.Add(Queue, client.PullModel().GetAwaiter().GetResult());
            }
            else
            {
                nodeList = new List<string>();
                for (int i = 0; i < QueueCount; i++)
                {
                    var queue = $"{ Queue}_{i}";
                    nodeList.Add(queue);
                    models.Add(queue, client.PullModel().GetAwaiter().GetResult());
                }
                _CHash = new ConsistentHash(nodeList, QueueCount * 10);
            }
            //申明exchange
            client.ExchangeDeclare(Exchange).Wait();
            Client = client;
        }
        public (string queue, ModelWrapper model) GetQueue(string key)
        {
            var queue = QueueCount == 1 ? Queue : _CHash.GetNode(key);
            var model = models[queue];
            if (model.Model.IsClosed)
            {
                model.Dispose();
                models[queue] = Client.PullModel().GetAwaiter().GetResult();
            }
            return (queue, );
        }

        public string Exchange { get; set; }
        public string Queue { get; set; }
        public int QueueCount { get; set; }
    }
    public static class RabbitPubAttrExtensions
    {
        public static void Publish(this RabbitPubAttribute rabbitMQInfo, byte[] bytes, string key, bool persistent = true)
        {
            var (queue, model) = rabbitMQInfo.GetQueue(key);
            model.Publish(bytes, rabbitMQInfo.Exchange, queue, persistent);
        }
        public static void PublishByCmd<T>(this RabbitPubAttribute rabbitMQInfo, UInt16 cmd, T data, string key, bool persistent = true)
        {
            var (queue, model) = rabbitMQInfo.GetQueue(key);
            model.PublishByCmd<T>(cmd, data, rabbitMQInfo.Exchange, queue, persistent);
        }
    }
}
