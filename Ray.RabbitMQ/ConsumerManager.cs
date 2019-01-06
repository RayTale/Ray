using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Ray.Core.Abstractions;
using Ray.Core.Client;
using Ray.Core.EventBus;
using Ray.Core.Utils;

namespace Ray.EventBus.RabbitMQ
{
    public class ConsumerManager<W> : IConsumerManager
        where W : IBytesWrapper
    {
        readonly ILogger<ConsumerManager<W>> logger;
        readonly IRabbitMQClient client;
        readonly IRabbitEventBusContainer<W> rabbitEventBusContainer;
        readonly IEventBusStartup<W> eventBusStartup;
        public ConsumerManager(
            ILogger<ConsumerManager<W>> logger,
            IRabbitMQClient client,
            IServiceProvider provider,
            IEventBusStartup<W> eventBusStartup,
            IRabbitEventBusContainer<W> rabbitEventBusContainer)
        {
            this.client = client;
            this.logger = logger;
            this.eventBusStartup = eventBusStartup;
            this.rabbitEventBusContainer = rabbitEventBusContainer;
        }
        public async Task Start(string node, List<string> nodeList = null)
        {
            await eventBusStartup.ConfigureEventBus(rabbitEventBusContainer);
            var subscribers = rabbitEventBusContainer.GetConsumers();
            var hash = nodeList == null ? null : new ConsistentHash(nodeList);
            var consumerList = new SortedList<int, ConsumerInfo<W>>();
            var rd = new Random((int)DateTime.UtcNow.Ticks);
            foreach (var subscriber in subscribers)
            {
                if (subscriber is RabbitConsumer<W> sub)
                {
                    for (int i = 0; i < sub.QueueList.Count(); i++)
                    {
                        var queue = sub.QueueList[i];
                        var hashNode = hash != null && nodeList.Count > 1 ? hash.GetNode(queue.Queue) : node;
                        if (node == hashNode)
                        {
                            consumerList.Add(rd.Next(), new ConsumerInfo<W>
                            {
                                Consumer = sub,
                                Queue = queue
                            });
                        }
                    }
                }
            }
            await Start(consumerList.Values.ToList(), 1);
        }

        private List<ConsumerInfo<W>> ConsumerList { get; set; }
        protected Timer MonitorTimer { get; private set; }
        public async Task Start(List<ConsumerInfo<W>> consumerList, int delay = 0)
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
                    ClusterClientFactory.ReBuild();
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
                        if (consumer.NowQos < consumer.Consumer.MinQos)
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
                logger.LogError(exception.InnerException ?? exception, nameof(ConsumerManager<W>));
            }
        }
        private async Task StartSub(ConsumerInfo<W> consumer, bool first)
        {
            var child = new ConsumerChild
            {
                Channel = await client.PullModel(),
                Qos = consumer.Consumer.MinQos
            };
            if (first)
            {
                child.Channel.Model.ExchangeDeclare(consumer.Consumer.EventBus.Exchange, "direct", true);
                child.Channel.Model.QueueDeclare(consumer.Queue.Queue, true, false, false, null);
                child.Channel.Model.QueueBind(consumer.Queue.Queue, consumer.Consumer.EventBus.Exchange, consumer.Queue.RoutingKey);
            }
            child.Channel.Model.BasicQos(0, consumer.Consumer.MinQos, false);

            child.BasicConsumer = new EventingBasicConsumer(child.Channel.Model);
            child.BasicConsumer.Received += async (ch, ea) =>
            {
                await Process(consumer, child, ea, 0);
            };
            child.BasicConsumer.ConsumerTag = child.Channel.Model.BasicConsume(consumer.Queue.Queue, consumer.Consumer.AutoAck, child.BasicConsumer);
            child.NeedRestart = false;
            consumer.Children.Add(child);
            consumer.NowQos += child.Qos;
            consumer.StartTime = DateTime.UtcNow;
        }
        private async Task ExpandQos(ConsumerInfo<W> consumer)
        {
            if (consumer.NowQos + consumer.Consumer.IncQos <= consumer.Consumer.MaxQos)
            {
                var child = new ConsumerChild
                {
                    Channel = await client.PullModel(),
                    Qos = consumer.Consumer.IncQos
                };
                child.Channel.Model.BasicQos(0, consumer.Consumer.IncQos, false);

                child.BasicConsumer = new EventingBasicConsumer(child.Channel.Model);
                child.BasicConsumer.Received += async (ch, ea) =>
                {
                    await Process(consumer, child, ea, 0);
                };
                child.BasicConsumer.ConsumerTag = child.Channel.Model.BasicConsume(consumer.Queue.Queue, consumer.Consumer.AutoAck, child.BasicConsumer);
                child.NeedRestart = false;
                consumer.Children.Add(child);
                consumer.NowQos += child.Qos;
                consumer.StartTime = DateTime.UtcNow;
            }
        }
        private async Task Process(ConsumerInfo<W> consumer, ConsumerChild consumerChild, BasicDeliverEventArgs ea, int count)
        {
            if (count > 0)
                await Task.Delay(count * 1000);
            try
            {
                await consumer.Consumer.Notice(ea.Body);
                if (!consumer.Consumer.AutoAck)
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
                logger.LogError(exception.InnerException ?? exception, $"An error occurred in {consumer.Consumer.EventBus.Exchange}-{consumer.Queue}");
                if (consumer.Consumer.ErrorReject)
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
    public class ConsumerInfo<W> where W : IBytesWrapper
    {
        public RabbitConsumer<W> Consumer { get; set; }
        public QueueInfo Queue { get; set; }
        public ushort NowQos { get; set; }
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
