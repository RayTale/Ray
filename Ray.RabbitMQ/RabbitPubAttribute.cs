using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Ray.Core.Utils;

namespace Ray.RabbitMQ
{
    [AttributeUsage(AttributeTargets.Class)]
    public class RabbitPubAttribute : Attribute
    {
        public RabbitPubAttribute(string exchange = null, string queue = null, int queueCount = 1, bool cacheHashKey = false)
        {
            Exchange = exchange;
            Queue = queue;
            QueueCount = queueCount;
            CacheHashKey = cacheHashKey;
        }
        ConsistentHash _CHash;
        public IRabbitMQClient Client { get; set; }
        public bool CacheHashKey { get; set; }
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
                    if (!models.ContainsKey(queue))
                        models.Add(queue, client.PullModel().GetAwaiter().GetResult());
                }
                _CHash = new ConsistentHash(nodeList, QueueCount * 10);
            }
            //申明exchange
            client.ExchangeDeclare(Exchange).Wait();
            Client = client;
        }
        ConcurrentDictionary<string, (string queue, ModelWrapper model)> queueDict = new ConcurrentDictionary<string, (string queue, ModelWrapper model)>();
        public (string queue, ModelWrapper model) GetQueue(string key)
        {
            if (CacheHashKey)
            {
                if (!queueDict.TryGetValue(key, out var result))
                {
                    var queue = QueueCount == 1 ? Queue : _CHash.GetNode(key);
                    result = (queue, models[queue]);
                    queueDict.TryAdd(key, result);
                }
                if (result.model.Model.IsClosed)
                {
                    result.model.Dispose();
                    result.model = models[result.queue] = Client.PullModel().GetAwaiter().GetResult();
                }
                return result;
            }
            else
            {
                var queue = QueueCount == 1 ? Queue : _CHash.GetNode(key);
                var model = models[queue];
                if (model.Model.IsClosed)
                {
                    model.Dispose();
                    model = models[queue] = Client.PullModel().GetAwaiter().GetResult();
                }
                return (queue, model);
            }
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
