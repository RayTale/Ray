using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.EventBus;
using Ray.Core.Services.Abstractions;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.EventBus.Kafka
{
    public class ConsumerManager : IConsumerManager
    {
        readonly ILogger<ConsumerManager> logger;
        readonly IKafkaClient client;
        readonly IKafkaEventBusContainer kafkaEventBusContainer;
        readonly IServiceProvider provider;
        readonly IGrainFactory grainFactory;
        public ConsumerManager(
            ILogger<ConsumerManager> logger,
            IKafkaClient client,
            IGrainFactory grainFactory,
            IServiceProvider provider,
            IKafkaEventBusContainer kafkaEventBusContainer)
        {
            this.provider = provider;
            this.client = client;
            this.logger = logger;
            this.kafkaEventBusContainer = kafkaEventBusContainer;
            this.grainFactory = grainFactory;
        }
        private readonly ConcurrentDictionary<string, ConsumerRunner> ConsumerRunners = new ConcurrentDictionary<string, ConsumerRunner>();
        private ConcurrentDictionary<string, long> LockDict { get; } = new ConcurrentDictionary<string, long>();
        private Timer HeathCheckTimer { get; set; }
        private Timer DistributedMonitorTime { get; set; }
        private Timer DistributedHoldTimer { get; set; }
        const int lockHoldingSeconds = 60;
        int distributedMonitorTimeLock = 0;
        int distributedHoldTimerLock = 0;
        int heathCheckTimerLock = 0;

        public Task Start()
        {
            DistributedMonitorTime = new Timer(state => DistributedStart().Wait(), null, 1000, 60 * 2 * 1000);
            DistributedHoldTimer = new Timer(state => DistributedHold().Wait(), null, 20 * 1000, 20 * 1000);
            HeathCheckTimer = new Timer(state => { HeathCheck().Wait(); }, null, 5 * 1000, 10 * 1000);
            return Task.CompletedTask;
        }
        private async Task DistributedStart()
        {
            try
            {
                if (Interlocked.CompareExchange(ref distributedMonitorTimeLock, 1, 0) == 0)
                {
                    var consumers = kafkaEventBusContainer.GetConsumers();
                    foreach (var consumer in consumers)
                    {
                        if (consumer is KafkaConsumer value)
                        {
                            for (int i = 0; i < value.Topics.Count(); i++)
                            {
                                var topic = value.Topics[i];
                                var weight = 100000 - LockDict.Count;
                                var (isOk, lockId, expectMillisecondDelay) = await grainFactory.GetGrain<IWeightHoldLock>(topic).Lock(weight, lockHoldingSeconds);
                                if (isOk)
                                {
                                    if (LockDict.TryAdd(topic, lockId))
                                    {
                                        var runner = new ConsumerRunner(client, provider.GetService<ILogger<ConsumerRunner>>(), value, topic);
                                        ConsumerRunners.TryAdd(topic, runner);
                                        await runner.Run();
                                    }
                                }
                            }
                        }
                    }
                    Interlocked.Exchange(ref distributedMonitorTimeLock, 0);
                }
            }
            catch (Exception exception)
            {
                logger.LogError(exception.InnerException ?? exception, nameof(DistributedStart));
                Interlocked.Exchange(ref distributedMonitorTimeLock, 0);
            }
        }
        private async Task DistributedHold()
        {
            try
            {
                if (Interlocked.CompareExchange(ref distributedHoldTimerLock, 1, 0) == 0)
                {
                    foreach (var lockKV in LockDict)
                    {
                        if (LockDict.TryGetValue(lockKV.Key, out var lockId))
                        {
                            var holdResult = await grainFactory.GetGrain<IWeightHoldLock>(lockKV.Key).Hold(lockId, lockHoldingSeconds);
                            if (!holdResult)
                            {
                                if (ConsumerRunners.TryRemove(lockKV.Key, out var runner))
                                {
                                    runner.Close();
                                }
                                LockDict.TryRemove(lockKV.Key, out var _);
                            }
                        }
                    }
                    Interlocked.Exchange(ref distributedHoldTimerLock, 0);
                }
            }
            catch (Exception exception)
            {
                logger.LogError(exception.InnerException ?? exception, nameof(DistributedHold));
                Interlocked.Exchange(ref distributedHoldTimerLock, 0);
            }
        }
        private async Task HeathCheck()
        {
            try
            {
                if (Interlocked.CompareExchange(ref heathCheckTimerLock, 1, 0) == 0)
                {
                    await Task.WhenAll(ConsumerRunners.Values.Select(runner => runner.HeathCheck()));
                    Interlocked.Exchange(ref heathCheckTimerLock, 0);
                }
            }
            catch (Exception exception)
            {
                logger.LogError(exception.InnerException ?? exception, nameof(HeathCheck));
                Interlocked.Exchange(ref heathCheckTimerLock, 0);
            }
        }
        public void Stop()
        {
            foreach (var runner in ConsumerRunners.Values)
            {
                runner.Close();
            }
            HeathCheckTimer?.Dispose();
            DistributedMonitorTime?.Dispose();
            DistributedHoldTimer?.Dispose();
        }
    }
}
