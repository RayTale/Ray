using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ray.Core.Abstractions;
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
        readonly IServiceProvider provider;
        public ConsumerManager(
            ILogger<ConsumerManager<W>> logger,
            IRabbitMQClient client,
            IServiceProvider provider,
            IRabbitEventBusContainer<W> rabbitEventBusContainer)
        {
            this.provider = provider;
            this.client = client;
            this.logger = logger;
            this.rabbitEventBusContainer = rabbitEventBusContainer;
        }
        public async Task Start(string node, List<string> nodeList = null)
        {
            var consumers = rabbitEventBusContainer.GetConsumers();
            var hash = nodeList == null ? null : new ConsistentHash(nodeList);
            var consumerList = new SortedList<int, ConsumerRunner<W>>();
            var rd = new Random((int)DateTime.UtcNow.Ticks);
            foreach (var consumer in consumers)
            {
                if (consumer is RabbitConsumer<W> value)
                {
                    for (int i = 0; i < value.QueueList.Count(); i++)
                    {
                        var queue = value.QueueList[i];
                        var hashNode = hash != null && nodeList.Count > 1 ? hash.GetNode(queue.Queue) : node;
                        if (node == hashNode)
                        {
                            consumerList.Add(rd.Next(), new ConsumerRunner<W>(client, provider.GetService<ILogger<ConsumerRunner<W>>>(), value, queue));
                        }
                    }
                }
            }
            await Start(consumerList.Values.ToList(), 1);
        }

        private List<ConsumerRunner<W>> ConsumerList { get; set; }
        protected Timer MonitorTimer { get; private set; }
        private async Task Start(List<ConsumerRunner<W>> consumerList, int delay = 0)
        {
            if (consumerList != null)
            {
                for (int i = 0; i < consumerList.Count; i++)
                {
                    await consumerList[i].Run();
                    if (delay != 0)
                        await Task.Delay(delay * 500);
                }
                ConsumerList = consumerList;
                MonitorTimer = new Timer(state => { HeathCheck().Wait(); }, null, 5 * 1000, 10 * 1000);
            }
        }
        private async Task HeathCheck()
        {
            try
            {
                foreach (var consumer in ConsumerList)
                {
                    await consumer.HeathCheck();
                }
            }
            catch (Exception exception)
            {
                logger.LogError(exception.InnerException ?? exception, nameof(ConsumerManager<W>));
            }
        }
        public void Stop()
        {
            if (ConsumerList != null)
            {
                foreach (var consumer in ConsumerList)
                {
                    foreach (var child in consumer.Slices)
                    {
                        child.NeedRestart = false;
                    }
                    consumer.Close();
                }
            }
        }
    }
}
