using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ray.Core.Utils;

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
        List<string> nodeList = new List<string>();
        public void Init(IRabbitMQClient client)
        {
            for (int i = 0; i < QueueCount; i++)
            {
                nodeList.Add(Queue + "_" + i);
            }
            _CHash = new ConsistentHash(nodeList, QueueCount * 10);
            //申明exchange
            client.ExchangeDeclare(Exchange).Wait();
            Client = client;
        }
        public string GetQueue(string key)
        {
            if (QueueCount == 1) return Queue;

            return _CHash.GetNode(key);
        }
        public string Exchange { get; set; }
        public string Queue { get; set; }
        public int QueueCount { get; set; }
    }
    public static class RabbitPubAttrExtensions
    {
        public static async Task Publish(this RabbitPubAttribute rabbitMQInfo, byte[] bytes, string key, bool persistent = true)
        {
            await rabbitMQInfo.Client.Publish(bytes, rabbitMQInfo.Exchange, rabbitMQInfo.GetQueue(key), persistent);
        }
        public static async Task PublishByCmd<T>(this RabbitPubAttribute rabbitMQInfo, UInt16 cmd, T data, string key, bool persistent = true)
        {
            await rabbitMQInfo.Client.PublishByCmd<T>(cmd, data, rabbitMQInfo.Exchange, rabbitMQInfo.GetQueue(key), persistent);
        }
    }
}
