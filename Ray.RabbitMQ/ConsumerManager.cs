using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ray.Core.Client;
using Ray.Core.EventBus;
using Ray.Core.IGrains;
using Ray.Core.Serialization;
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
        readonly RabbitEventBusOptions rabbitEventBusOptions;
        readonly IClusterClientFactory clusterClientFactory;
        public ConsumerManager(
            ILogger<ConsumerManager<W>> logger,
            IRabbitMQClient client,
            IClusterClientFactory clusterClientFactory,
            IServiceProvider provider,
            IOptions<RabbitEventBusOptions> rabbitEventBusOptions,
            IRabbitEventBusContainer<W> rabbitEventBusContainer)
        {
            this.provider = provider;
            this.client = client;
            this.logger = logger;
            this.rabbitEventBusOptions = rabbitEventBusOptions.Value;
            this.rabbitEventBusContainer = rabbitEventBusContainer;
            this.clusterClientFactory = clusterClientFactory;
        }
        private List<ConsumerRunner<W>> ConsumerList { get; } = new List<ConsumerRunner<W>>();
        private Timer HeathCheckTimer { get; set; }
        private Timer DistributedMonitorTime { get; set; }
        private List<StartedNode> StartedNodeList { get; } = new List<StartedNode>();
        public async Task Start()
        {
            if (rabbitEventBusOptions.Nodes != default && rabbitEventBusOptions.Nodes.Length > 0)
            {
                await DistributedStart();
                DistributedMonitorTime = new Timer(state => DistributedStart().Wait(), null, 60 * 1000, 60 * 1000);
            }
            else
            {
                await Start(null, null);
            }
            HeathCheckTimer = new Timer(state => { HeathCheck().Wait(); }, null, 5 * 1000, 10 * 1000);
        }
        private async Task DistributedStart()
        {
            try
            {
                foreach (var node in rabbitEventBusOptions.Nodes.Where(node => !StartedNodeList.Exists(s => s.Node == node)))
                {
                    var (isOk, lockId) = await clusterClientFactory.Create().GetGrain<INoWaitLock>(node).Lock(60);
                    if (isOk)
                    {
                        await Start(node, rabbitEventBusOptions.Nodes);
                        StartedNodeList.Add(new StartedNode { Node = node, LockId = lockId });
                        break;
                    }
                }
            }
            catch (Exception exception)
            {
                logger.LogError(exception.InnerException ?? exception, nameof(ConsumerManager<W>));
            }
        }
        private async Task Start(string node = null, string[] nodeList = null)
        {
            var consumers = rabbitEventBusContainer.GetConsumers();
            var hash = nodeList == null || nodeList.Length == 0 ? null : new ConsistentHash(nodeList);
            var consumerList = new SortedList<int, ConsumerRunner<W>>();
            var rd = new Random((int)DateTime.UtcNow.Ticks);
            foreach (var consumer in consumers)
            {
                if (consumer is RabbitConsumer<W> value)
                {
                    for (int i = 0; i < value.QueueList.Count(); i++)
                    {
                        var queue = value.QueueList[i];
                        var hashNode = hash != null && nodeList.Length > 1 ? hash.GetNode(queue.Queue) : node;
                        if (node == hashNode)
                        {
                            consumerList.Add(rd.Next(), new ConsumerRunner<W>(client, provider.GetService<ILogger<ConsumerRunner<W>>>(), value, queue));
                        }
                    }
                }
            }
            await Start(consumerList.Values.ToList());
        }
        private async Task Start(List<ConsumerRunner<W>> consumerList)
        {
            if (consumerList != null)
            {
                for (int i = 0; i < consumerList.Count; i++)
                {
                    await consumerList[i].Run();
                    await Task.Delay(rabbitEventBusOptions.QueueStartMillisecondsDelay);
                }
                ConsumerList.AddRange(consumerList);
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
