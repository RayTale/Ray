using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.Services;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.EventBus.RabbitMQ
{
    public class ConsumerManager : IHostedService, IDisposable
    {
        readonly ILogger<ConsumerManager> logger;
        readonly IRabbitMQClient client;
        readonly IRabbitEventBusContainer rabbitEventBusContainer;
        readonly IServiceProvider provider;
        readonly IGrainFactory grainFactory;
        const int _HoldTime = 20 * 1000;
        const int _MonitTime = 60 * 2 * 1000;
        const int _checkTime = 10 * 1000;

        public ConsumerManager(
            ILogger<ConsumerManager> logger,
            IRabbitMQClient client,
            IGrainFactory grainFactory,
            IServiceProvider provider,
            IRabbitEventBusContainer rabbitEventBusContainer)
        {
            this.provider = provider;
            this.client = client;
            this.logger = logger;
            this.rabbitEventBusContainer = rabbitEventBusContainer;
            this.grainFactory = grainFactory;
        }
        private readonly ConcurrentDictionary<string, ConsumerRunner> ConsumerRunners = new ConcurrentDictionary<string, ConsumerRunner>();
        private ConcurrentDictionary<string, long> Runners { get; } = new ConcurrentDictionary<string, long>();
        private Timer HeathCheckTimer { get; set; }
        private Timer DistributedMonitorTime { get; set; }
        private Timer DistributedHoldTimer { get; set; }
        const int lockHoldingSeconds = 60;
        int distributedMonitorTimeLock = 0;
        int distributedHoldTimerLock = 0;
        int heathCheckTimerLock = 0;
        private async Task DistributedStart()
        {
            try
            {
                if (Interlocked.CompareExchange(ref distributedMonitorTimeLock, 1, 0) == 0)
                {
                    var consumers = rabbitEventBusContainer.GetConsumers();
                    foreach (var consumer in consumers)
                    {
                        if (consumer is RabbitConsumer value)
                        {
                            for (int i = 0; i < value.QueueList.Count(); i++)
                            {
                                var queue = value.QueueList[i];
                                var key = queue.ToString();
                                if (!Runners.ContainsKey(key))
                                {
                                    var weight = 100000 - Runners.Count;
                                    var (isOk, lockId, expectMillisecondDelay) = await grainFactory.GetGrain<IWeightHoldLock>(key).Lock(weight, lockHoldingSeconds);
                                    if (isOk)
                                    {
                                        if (Runners.TryAdd(key, lockId))
                                        {
                                            var runner = new ConsumerRunner(client, provider, value, queue);
                                            ConsumerRunners.TryAdd(key, runner);
                                            await runner.Run();
                                        }

                                    }
                                }
                            }
                        }
                    }
                    Interlocked.Exchange(ref distributedMonitorTimeLock, 0);
                    if (logger.IsEnabled(LogLevel.Information))
                        logger.LogInformation("EventBus Background Service is working.");
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
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation("EventBus Background Service is holding.");
                if (Interlocked.CompareExchange(ref distributedHoldTimerLock, 1, 0) == 0)
                {
                    foreach (var lockKV in Runners)
                    {
                        if (Runners.TryGetValue(lockKV.Key, out var lockId))
                        {
                            var holdResult = await grainFactory.GetGrain<IWeightHoldLock>(lockKV.Key).Hold(lockId, lockHoldingSeconds);
                            if (!holdResult)
                            {
                                if (ConsumerRunners.TryRemove(lockKV.Key, out var runner))
                                {
                                    runner.Close();
                                }
                                Runners.TryRemove(lockKV.Key, out var _);
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
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation("EventBus Background Service is checking.");
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
        public Task StartAsync(CancellationToken cancellationToken)
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("EventBus Background Service is starting.");
            DistributedMonitorTime = new Timer(state => DistributedStart().Wait(), null, 1000, _MonitTime);
            DistributedHoldTimer = new Timer(state => DistributedHold().Wait(), null, _HoldTime, _HoldTime);
            HeathCheckTimer = new Timer(state => { HeathCheck().Wait(); }, null, _checkTime, _checkTime);
            return Task.CompletedTask;
        }
        public Task StopAsync(CancellationToken cancellationToken)
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("EventBus Background Service is stopping.");
            Dispose();
            return Task.CompletedTask;
        }
        public void Dispose()
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("EventBus Background Service is disposing.");
            foreach (var runner in ConsumerRunners.Values)
            {
                runner.Close();
            }
            DistributedMonitorTime?.Dispose();
            DistributedHoldTimer?.Dispose();
            HeathCheckTimer?.Dispose();
        }
    }
}
