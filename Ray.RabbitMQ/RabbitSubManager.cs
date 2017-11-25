using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Ray.Core.MQ;
using Ray.Core.Lib;

namespace Ray.RabbitMQ
{
    public class RabbitSubManager : SubManager
    {
        ILogger<RabbitSubManager> logger = default;
        public RabbitSubManager(ILogger<RabbitSubManager> logger) => this.logger = logger;
        protected override async Task Start(List<SubAttribute> attributes, IServiceProvider provider, string node, List<string> nodeList = null)
        {
            var hash = nodeList == null ? null : new ConsistentHash(nodeList);
            var consumerList = new List<ConsumerInfo>();
            foreach (var attribute in attributes)
            {
                if (attribute is RabbitSubAttribute value)
                {

                    for (int i = 0; i < value.QueueList.Count(); i++)
                    {
                        var queue = value.QueueList[i];
                        var hashNode = hash != null ? hash.GetNode(queue.Queue) : node;
                        if (node == hashNode)
                        {
                            consumerList.Add(new ConsumerInfo()
                            {
                                Exchange = value.Exchange,
                                Queue = (string.IsNullOrEmpty(node) ? string.Empty : node + "_") + value.Type + "_" + queue.Queue,
                                RoutingKey = queue.RoutingKey,
                                Handler = (ISubHandler)provider.GetService(value.Handler)
                            });
                        }
                    }

                }
            }
            await Start(consumerList);
        }

        static ConcurrentBag<ConsumerInfo> ConsumerAllList = new ConcurrentBag<ConsumerInfo>();
        public async Task Start(List<ConsumerInfo> consumerList)
        {
            if (consumerList != null)
            {
                var channel = await RabbitMQClient.PullModel();

                for (int i = 0; i < consumerList.Count; i++)
                {
                    var consumer = consumerList[i];
                    consumer.Channel = channel;
                    channel.Model.ExchangeDeclare(consumer.Exchange, "direct", true);
                    channel.Model.QueueDeclare(consumer.Queue, true, false, false, null);
                    channel.Model.QueueBind(consumer.Queue, consumer.Exchange, consumer.RoutingKey);
                    consumer.BasicConsumer = new EventingBasicConsumer(consumer.Channel.Model);
                    consumer.BasicConsumer.Received += (ch, ea) =>
                    {
                        try
                        {
                            consumer.Handler.Notice(ea.Body).ContinueWith(t =>
                            {
                                if (t.Exception == null && !t.IsCanceled)
                                {
                                    consumer.Channel.Model.BasicAck(ea.DeliveryTag, false);
                                }
                                else if (t.Exception != null)
                                {
                                    throw t.Exception;
                                }
                                else if (t.IsCanceled)
                                {
                                    throw new Exception("消息处理超时");
                                }
                            }).GetAwaiter().GetResult();
                        }
                        catch (Exception exception)
                        {
                            //需要记录错误日志
                            var e = exception.InnerException ?? exception;
                            logger.LogError(e, $"消息队列消息处理失败，失败的队列为{consumer.Exchange}-{consumer.Queue}");
                            ReStart(consumer);//重启队列
                        }
                    };
                    consumer.BasicConsumer.ConsumerTag = consumer.Channel.Model.BasicConsume(consumer.Queue, false, consumer.BasicConsumer);
                    if (i % 4 == 0 && i != 0)
                    {
                        channel = await RabbitMQClient.PullModel();
                    }
                    if (!ConsumerAllList.Contains(consumer))
                    {
                        ConsumerAllList.Add(consumer);
                    }
                }
            }
        }
        /// <summary>
        /// 重启消费者
        /// </summary>
        /// <param name="consumer"></param>
        /// <returns></returns>
        public static void ReStart(ConsumerInfo consumer)
        {
            if (consumer.Channel.Model.IsOpen)
            {
                consumer.Channel.Model.BasicCancel(consumer.BasicConsumer.ConsumerTag);
                consumer.BasicConsumer.ConsumerTag = consumer.Channel.Model.BasicConsume(consumer.Queue, false, consumer.BasicConsumer);
            }
        }
    }
    public class ConsumerInfo
    {
        public string Exchange { get; set; }
        public string Queue { get; set; }
        public string RoutingKey { get; set; }
        public ISubHandler Handler { get; set; }
        public ModelWrapper Channel { get; set; }
        public EventingBasicConsumer BasicConsumer { get; set; }
    }
}
