using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Ray.Core.Client;
using Ray.Core.EventBus;
using Ray.Core.Utils;

namespace Ray.RabbitMQ
{
    public class RabbitSubManager : ISubManager
    {
        readonly ILogger<RabbitSubManager> logger;
        readonly IServiceProvider provider;
        readonly IRabbitMQClient client;
        public RabbitSubManager(ILogger<RabbitSubManager> logger, IRabbitMQClient client, IServiceProvider provider)
        {
            this.client = client;
            this.logger = logger;
            this.provider = provider;
        }
        public async Task Start(List<Subscriber> subscribers, string group, string node, List<string> nodeList = null)
        {
            var hash = nodeList == null ? null : new ConsistentHash(nodeList);
            var consumerList = new SortedList<int, ConsumerInfo>();
            var rd = new Random((int)DateTime.UtcNow.Ticks);
            foreach (var subscriber in subscribers)
            {
                if (subscriber is RabbitSubscriber sub)
                {
                    await sub.Init(client);
                    for (int i = 0; i < sub.QueueList.Count(); i++)
                    {
                        var queue = sub.QueueList[i];
                        var hashNode = hash != null ? hash.GetNode(queue.Queue) : node;
                        if (node == hashNode)
                        {
                            consumerList.Add(rd.Next(), new ConsumerInfo()
                            {
                                Exchange = sub.Exchange,
                                Queue = $"{group}_{queue.Queue}",
                                RoutingKey = queue.RoutingKey,
                                MaxQos = sub.MaxQos,
                                MinQos = sub.MinQos,
                                IncQos = sub.IncQos,
                                ErrorReject = sub.ErrorReject,
                                AutoAck = sub.AutoAck,
                                Handler = (ISubHandler)provider.GetService(sub.Handler)
                            });
                        }
                    }
                }
            }
            await Start(consumerList.Values.ToList(), 1);
        }

        private List<ConsumerInfo> ConsumerList { get; set; }
        protected Timer MonitorTimer { get; private set; }
        public async Task Start(List<ConsumerInfo> consumerList, int delay = 0)
        {
            if (consumerList != null)
            {
                for (int i = 0; i < consumerList.Count; i++)
                {
                    await StartSub(consumerList[i], true);
                    if (delay != 0)
                        await Task.Delay(delay * 500);
                }
                ConsumerList = consumerList;
                MonitorTimer = new Timer(state => { Restart().Wait(); }, null, 5 * 1000, 10 * 1000);
            }
        }
        DateTime restartStatisticalStartTime = DateTime.UtcNow;
        int restartStatisticalCount = 0;
        private async Task Restart()
        {
            try
            {
                var nowTime = DateTime.UtcNow;
                restartStatisticalCount = restartStatisticalCount + ConsumerList.Where(consumer => consumer.Children.Any(c => c.NeedRestart)).Count();
                if (restartStatisticalCount > ConsumerList.Count / 3)
                {
                    ClientFactory.ReBuild();
                    restartStatisticalStartTime = nowTime;
                    restartStatisticalCount = 0;
                }
                else if ((nowTime - restartStatisticalStartTime).Minutes > 10)
                {
                    restartStatisticalStartTime = nowTime;
                    restartStatisticalCount = 0;
                }

                foreach (var consumer in ConsumerList)
                {
                    var needRestartChildren = consumer.Children.Where(child => child.NeedRestart || child.BasicConsumer == null || !child.BasicConsumer.IsRunning || child.Channel.Model.IsClosed).ToList();
                    if (needRestartChildren.Count > 0)
                    {
                        foreach (var child in needRestartChildren)
                        {
                            child.Close();
                            consumer.Children.Remove(child);
                            consumer.NowQos -= child.Qos;
                        }
                        if (consumer.NowQos < consumer.MinQos)
                        {
                            await StartSub(consumer, false);
                        }
                    }
                    else if ((nowTime - consumer.StartTime).TotalMinutes >= 5)
                    {
                        await ExpandQos(consumer);//扩容操作
                    }
                }
            }
            catch (Exception exception)
            {
                logger.LogError(exception.InnerException ?? exception, "消息队列守护线程发生错误");
            }
        }
        private async Task StartSub(ConsumerInfo consumer, bool first)
        {
            var child = new ConsumerChild
            {
                Channel = await client.PullModel(),
                Qos = consumer.MinQos
            };
            if (first)
            {
                child.Channel.Model.ExchangeDeclare(consumer.Exchange, "direct", true);
                child.Channel.Model.QueueDeclare(consumer.Queue, true, false, false, null);
                child.Channel.Model.QueueBind(consumer.Queue, consumer.Exchange, consumer.RoutingKey);
            }
            child.Channel.Model.BasicQos(0, consumer.MinQos, false);

            child.BasicConsumer = new EventingBasicConsumer(child.Channel.Model);
            child.BasicConsumer.Received += async (ch, ea) =>
            {
                await Process(consumer, child, ea, 0);
            };
            child.BasicConsumer.ConsumerTag = child.Channel.Model.BasicConsume(consumer.Queue, consumer.AutoAck, child.BasicConsumer);
            child.NeedRestart = false;
            consumer.Children.Add(child);
            consumer.NowQos += child.Qos;
            consumer.StartTime = DateTime.UtcNow;
        }
        private async Task ExpandQos(ConsumerInfo consumer)
        {
            if (consumer.NowQos + consumer.IncQos <= consumer.MaxQos)
            {
                var child = new ConsumerChild
                {
                    Channel = await client.PullModel(),
                    Qos = consumer.IncQos
                };
                child.Channel.Model.BasicQos(0, consumer.IncQos, false);

                child.BasicConsumer = new EventingBasicConsumer(child.Channel.Model);
                child.BasicConsumer.Received += async (ch, ea) =>
                {
                    await Process(consumer, child, ea, 0);
                };
                child.BasicConsumer.ConsumerTag = child.Channel.Model.BasicConsume(consumer.Queue, consumer.AutoAck, child.BasicConsumer);
                child.NeedRestart = false;
                consumer.Children.Add(child);
                consumer.NowQos += child.Qos;
                consumer.StartTime = DateTime.UtcNow;
            }
        }
        private async Task Process(ConsumerInfo consumer, ConsumerChild consumerChild, BasicDeliverEventArgs ea, int count)
        {
            if (count > 0)
                await Task.Delay(count * 1000);
            try
            {
                await consumer.Handler.Notice(ea.Body);
                if (!consumer.AutoAck)
                {
                    try
                    {
                        consumerChild.Channel.Model.BasicAck(ea.DeliveryTag, false);
                    }
                    catch
                    {
                        consumerChild.NeedRestart = true;
                    }
                }
            }
            catch (Exception exception)
            {
                logger.LogError(exception.InnerException ?? exception, $"An error occurred in {consumer.Exchange}-{consumer.Queue}");
                if (consumer.ErrorReject)
                {
                    consumerChild.Channel.Model.BasicReject(ea.DeliveryTag, true);
                }
                else
                {
                    if (count > 3)
                        consumerChild.NeedRestart = true;
                    else
                        await Process(consumer, consumerChild, ea, count + 1);
                }
            }
        }
        public void Stop()
        {
            if (ConsumerList != null)
            {
                foreach (var consumer in ConsumerList)
                {
                    foreach (var child in consumer.Children)
                    {
                        child.NeedRestart = false;
                    }
                    consumer.Close();
                }
            }
        }
    }
    public class ConsumerInfo
    {
        public string Exchange { get; set; }
        public string Queue { get; set; }
        public string RoutingKey { get; set; }
        public ushort MaxQos { get; set; }
        public ushort MinQos { get; set; }
        public ushort IncQos { get; set; }
        public ushort NowQos { get; set; }
        public bool AutoAck { get; set; }
        public bool ErrorReject { get; set; }
        public ISubHandler Handler { get; set; }
        public List<ConsumerChild> Children { get; set; } = new List<ConsumerChild>();
        public DateTime StartTime { get; set; }
        public void Close()
        {
            foreach (var child in Children)
            {
                child.Close();
            }
        }
    }
    public class ConsumerChild
    {
        public ModelWrapper Channel { get; set; }
        public EventingBasicConsumer BasicConsumer { get; set; }
        public ushort Qos { get; set; }
        public bool NeedRestart { get; set; }
        public void Close()
        {
            if (Channel != default && Channel.Model.IsOpen)
            {
                if (!NeedRestart)
                    BasicConsumer.Model.Abort();
                else
                    BasicConsumer.Model.Close();
                BasicConsumer.Model.Dispose();
            }
            Channel?.Dispose();
        }
    }
}
