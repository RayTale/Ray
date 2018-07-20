using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Ray.Core;
using Ray.Core.MQ;
using Ray.Core.Utils;

namespace Ray.RabbitMQ
{
    public class RabbitSubManager : SubManager
    {
        ILogger<RabbitSubManager> logger = default;
        IServiceProvider provider;
        IRabbitMQClient client;
        static Timer monitorTimer;
        public RabbitSubManager(ILogger<RabbitSubManager> logger, IRabbitMQClient client, IServiceProvider provider)
        {
            this.client = client;
            this.logger = logger;
            this.provider = provider;
        }
        protected override async Task Start(List<SubAttribute> attributes, string node, List<string> nodeList = null)
        {
            var hash = nodeList == null ? null : new ConsistentHash(nodeList);
            var consumerList = new SortedList<int, ConsumerInfo>();
            var rd = new Random((int)DateTime.UtcNow.Ticks);
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
                            consumerList.Add(rd.Next(), new ConsumerInfo()
                            {
                                Exchange = subAttribute.Exchange,
                                Queue = $"{ subAttribute.Group}_{queue.Queue}",
                                RoutingKey = queue.RoutingKey,
                                MaxQos = subAttribute.MaxQos,
                                MinQos = subAttribute.MinQos,
                                IncQos = subAttribute.IncQos,
                                ErrorReject = subAttribute.ErrorReject,
                                AutoAck = subAttribute.AutoAck,
                                Handler = (ISubHandler)provider.GetService(subAttribute.Handler)
                            });
                        }
                    }
                }
            }
            await Start(consumerList.Values.ToList(), 1);
        }

        private List<ConsumerInfo> ConsumerList { get; set; }
        public async Task Start(List<ConsumerInfo> consumerList, int delay = 0)
        {
            if (consumerList != null)
            {
                for (int i = 0; i < consumerList.Count; i++)
                {
                    await StartSub(consumerList[i]);
                    if (delay != 0)
                        await Task.Delay(delay * 500);
                }
                ConsumerList = consumerList;
                monitorTimer = new Timer(state => { Restart().Wait(); }, null, 5 * 1000, 10 * 1000);
            }
        }
        DateTime restartStatisticalStartTime = DateTime.UtcNow;
        int restartStatisticalCount = 0;
        private async Task Restart()
        {
            try
            {
                var nowTime = DateTime.UtcNow;
                restartStatisticalCount = restartStatisticalCount + ConsumerList.Where(consumer => consumer.NeedRestart).Count();
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
                    if (consumer.NeedRestart ||
                        consumer.BasicConsumer == null ||
                        !consumer.BasicConsumer.IsRunning ||
                        consumer.Channel.Model.IsClosed)
                    {
                        await StartSub(consumer);
                    }
                    else if ((nowTime - consumer.StartTime).TotalMinutes >= 5)
                    {
                        await StartSub(consumer, true);//扩容操作
                    }
                }
            }
            catch (Exception exception)
            {
                logger.LogError(exception.InnerException ?? exception, "消息队列守护线程发生错误");
            }
        }
        private async Task StartSub(ConsumerInfo consumer, bool expand = false)
        {
            if (expand && consumer.NowQos == consumer.MaxQos) return;
            if (consumer.BasicConsumer != null)
            {
                consumer.Close();
            }
            consumer.Channel = await client.PullModel();
            if (consumer.NowQos == 0)
            {
                consumer.Channel.Model.ExchangeDeclare(consumer.Exchange, "direct", true);
                consumer.Channel.Model.QueueDeclare(consumer.Queue, true, false, false, null);
                consumer.Channel.Model.QueueBind(consumer.Queue, consumer.Exchange, consumer.RoutingKey);
            }
            consumer.NowQos = expand ? (ushort)(consumer.NowQos + consumer.IncQos) : consumer.MinQos;
            if (consumer.NowQos > consumer.MaxQos) consumer.NowQos = consumer.MaxQos;

            consumer.Channel.Model.BasicQos(0, consumer.NowQos, false);

            consumer.BasicConsumer = new EventingBasicConsumer(consumer.Channel.Model);
            consumer.BasicConsumer.Received += async (ch, ea) =>
            {
                await Process(consumer, ea, 0);
            };
            consumer.BasicConsumer.ConsumerTag = consumer.Channel.Model.BasicConsume(consumer.Queue, consumer.AutoAck, consumer.BasicConsumer);
            consumer.StartTime = DateTime.UtcNow;
            consumer.NeedRestart = false;
        }
        private async Task Process(ConsumerInfo consumer, BasicDeliverEventArgs ea, int count)
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
                        consumer.Channel.Model.BasicAck(ea.DeliveryTag, false);
                    }
                    catch
                    {
                        consumer.NeedRestart = true;
                    }
                }
            }
            catch (Exception exception)
            {
                logger.LogError(exception.InnerException ?? exception, $"An error occurred in {consumer.Exchange}-{consumer.Queue}");
                if (consumer.ErrorReject)
                {
                    consumer.Channel.Model.BasicReject(ea.DeliveryTag, true);
                }
                else
                {
                    if (count > 3)
                        consumer.NeedRestart = true;
                    else
                        await Process(consumer, ea, count + 1);
                }
            }
        }
        public override void Stop()
        {
            if (ConsumerList != null)
            {
                foreach (var consumer in ConsumerList)
                {
                    consumer.NeedRestart = false;
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
        public bool NeedRestart { get; set; }
        public ISubHandler Handler { get; set; }
        public ModelWrapper Channel { get; set; }
        public EventingBasicConsumer BasicConsumer { get; set; }
        public DateTime StartTime { get; set; }
        public void Close()
        {
            if (Channel != default && Channel.Model.IsOpen)
            {
                if (!NeedRestart)
                    BasicConsumer.Model.Abort();
                else
                    BasicConsumer.Model.Close();
                BasicConsumer.Model.Dispose();
                BasicConsumer = null;
            }
            Channel?.Dispose();
        }
    }
}
