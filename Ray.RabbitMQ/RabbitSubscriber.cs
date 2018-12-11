using Ray.Core.EventBus;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.RabbitMQ
{
    public class RabbitSubscriber : Subscriber
    {
        readonly List<string> originQueueList;
        readonly string queue;
        public RabbitSubscriber(Type handler, string exchange, string queue, int queueCount = 1, ushort minQos = 150, ushort incQos = 20, ushort maxQos = 200, bool autoAck = false, bool errorReject = false) : base(handler)
        {
            Exchange = exchange;
            QueueCount = queueCount;
            this.queue = queue;
            AutoAck = autoAck;
            MaxQos = maxQos;
            MinQos = minQos;
            IncQos = incQos;
            ErrorReject = errorReject;
        }
        public RabbitSubscriber(Type handler, string exchange, List<string> queueList, ushort minQos = 20, ushort incQos = 20, ushort maxQos = 200, bool autoAck = false) : base(handler)
        {
            Exchange = exchange;
            originQueueList = queueList;
            AutoAck = autoAck;
            MaxQos = maxQos;
            MinQos = minQos;
            IncQos = incQos;
        }
        QueueInfo BuildQueueInfo(string queue)
        {
            return new QueueInfo()
            {
                RoutingKey = queue,
                Queue = queue
            };
        }
        public Task Init(IRabbitMQClient client)
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
            return client.ExchangeDeclare(this.Exchange);
        }
        public List<QueueInfo> QueueList { get; set; }
        public string Exchange { get; set; }
        public int QueueCount { get; set; }
        public bool AutoAck { get; set; }
        public bool ErrorReject { get; set; }
        public ushort MinQos { get; set; }
        public ushort IncQos { get; set; }
        public ushort MaxQos { get; set; }
    }
    public class QueueInfo
    {
        public string Queue { get; set; }
        public string RoutingKey { get; set; }
    }
}
