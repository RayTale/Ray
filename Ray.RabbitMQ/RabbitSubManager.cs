using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Ray.Core.MQ;
using Ray.Core.Utils;

namespace Ray.RabbitMQ
{
    public class RabbitSubManager : SubManager
    {
        ILogger<RabbitSubManager> logger = default;
        IServiceProvider provider;
        IRabbitMQClient client;
        public RabbitSubManager(ILogger<RabbitSubManager> logger, IRabbitMQClient client, IServiceProvider provider)
        {
            this.client = client;
            this.logger = logger;
            this.provider = provider;
        }
        protected override async Task Start(List<SubAttribute> attributes, string node, List<string> nodeList = null)
        {
            var hash = nodeList == null ? null : new ConsistentHash(nodeList);
            var consumerList = new List<ConsumerInfo>();
            foreach (var attribute in attributes)
            {
                if (attribute is RabbitSubAttribute subAttribute)
                {
                    subAttribute.Init(client);
                    for (int i = 0; i < subAttribute.QueueList.Count(); i++)
                    {
                        var queue = subAttribute.QueueList[i];
                        var hashNode = hash != null ? hash.GetNode(queue.Queue) : node;
                        if (node == hashNode)
                        {
                            for (int x = 0; x < 10; x++)
                            {
                                consumerList.Add(new ConsumerInfo()
                                {
                                    Exchange = subAttribute.Exchange,
                                    Queue = subAttribute.Group + "_" + queue.Queue,
                                    RoutingKey = queue.RoutingKey,
                                    Handler = (ISubHandler)provider.GetService(subAttribute.Handler)
                                });
                            }
                        }
                    }
                }
            }
            await Start(consumerList);
        }

        List<ConsumerInfo> ConsumerList { get; set; }
        ConsistentHash _CHash;
        Dictionary<string, ModelWrapper> modelDict = new Dictionary<string, ModelWrapper>();
        public async Task Start(List<ConsumerInfo> consumerList)
        {
            if (consumerList != null)
            {
                for (int i = 0; i < (int)Math.Ceiling(consumerList.Count / 4.0); i++)
                {
                    modelDict.Add(i.ToString(), await client.PullModel());
                }
                _CHash = new ConsistentHash(modelDict.Keys, modelDict.Keys.Count * 5);
                for (int i = 0; i < consumerList.Count; i++)
                {
                    var consumer = consumerList[i];
                    await StartSub(consumer);
                }
                ConsumerList = consumerList;
            }
        }
        private async Task StartSub(ConsumerInfo consumer)
        {
            consumer.Channel = await GetModel(consumer);
            consumer.BasicConsumer = new EventingBasicConsumer(consumer.Channel.Model);
            consumer.BasicConsumer.Received += async (ch, ea) =>
            {
                try
                {
                    await consumer.Handler.Notice(ea.Body).ContinueWith(t =>
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
                            throw new Exception("Message processing timeout");
                        }
                    });
                }
                catch (Exception exception)
                {
                    //需要记录错误日志
                    var e = exception.InnerException ?? exception;
                    logger.LogError(e, $"An error occurred in {consumer.Exchange}-{consumer.Queue}");
                    await ReStart(consumer);//重启队列
                }
            };
            consumer.BasicConsumer.ConsumerTag = consumer.Channel.Model.BasicConsume(consumer.Queue, false, consumer.BasicConsumer);
        }
        private async Task<ModelWrapper> GetModel(ConsumerInfo consumer)
        {
            var key = _CHash.GetNode(consumer.Queue);
            var channel = modelDict[key];
            if (channel.Model.IsClosed)
            {
                channel.Model.Dispose();
                modelDict[key] = channel = await client.PullModel();
            }
            channel.Model.ExchangeDeclare(consumer.Exchange, "direct", true);
            channel.Model.QueueDeclare(consumer.Queue, true, false, false, null);
            channel.Model.QueueBind(consumer.Queue, consumer.Exchange, consumer.RoutingKey);
            return channel;
        }
        /// <summary>
        /// 重启消费者
        /// </summary>
        /// <param name="consumer"></param>
        /// <returns></returns>
        public async Task ReStart(ConsumerInfo consumer)
        {
            if (consumer.Channel.Model.IsOpen)
            {
                consumer.Channel.Model.BasicCancel(consumer.BasicConsumer.ConsumerTag);
                consumer.BasicConsumer.ConsumerTag = consumer.Channel.Model.BasicConsume(consumer.Queue, false, consumer.BasicConsumer);
            }
            else
            {
                await StartSub(consumer);
            }
        }
        public override void Stop()
        {
            if (ConsumerList != null)
            {
                foreach (var consumer in ConsumerList)
                {
                    if (consumer.Channel.Model.IsOpen)
                    {
                        consumer.Channel.Model.BasicCancel(consumer.BasicConsumer.ConsumerTag);
                    }
                }
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
