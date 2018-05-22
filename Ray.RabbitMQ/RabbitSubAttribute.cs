using Ray.Core.MQ;
using System;
using System.Collections.Generic;

namespace Ray.RabbitMQ
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class RabbitSubAttribute : SubAttribute
    {
        readonly List<string> originQueueList;
        readonly string queue;
        public RabbitSubAttribute(string group, string exchange, string queue, int queueCount = 1, bool autoAck = false) : base(group)
        {
            Exchange = exchange;
            QueueCount = queueCount;
            this.queue = queue;
            AutoAck = autoAck;
        }
        public RabbitSubAttribute(string type, string exchange, List<string> queueList, bool autoAck = false) : base(type)
        {
            Exchange = exchange;
            originQueueList = queueList;
            AutoAck = autoAck;
        }
        QueueInfo BuildQueueInfo(string queue)
        {
            return new QueueInfo()
            {
                RoutingKey = queue,
                Queue = queue
            };
        }
        public void Init(IRabbitMQClient client)
        {
            QueueList = new List<QueueInfo>();
            if (originQueueList?.Count > 0)
            {
                foreach (var q in originQueueList)
                {
                    QueueList.Add(BuildQueueInfo(q));
                }
            }
            else if (!string.IsNullOrEmpty(queue))
            {
                if (QueueCount == 1)
                {
                    QueueList.Add(BuildQueueInfo(queue));
                }
                else
                {
                    for (int i = 0; i < QueueCount; i++)
                    {
                        QueueList.Add(BuildQueueInfo(queue + "_" + i));
                    }
                }
            }
            //申明exchange
            client.ExchangeDeclare(this.Exchange).Wait();
        }
        public List<QueueInfo> QueueList { get; set; }
        public string Exchange { get; set; }
        public int QueueCount { get; set; }
        public bool AutoAck { get; set; }
    }
    public class QueueInfo
    {
        public string Queue { get; set; }
        public string RoutingKey { get; set; }
    }
}
