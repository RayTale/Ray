using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.Core.Services;

namespace Ray.EventBus.RabbitMQ
{
    public class ConsumerManager : IHostedService, IDisposable
    {
        private readonly ILogger<ConsumerManager> logger;
        private readonly IRabbitMQClient client;
        private readonly IRabbitEventBusContainer rabbitEventBusContainer;
        private readonly IServiceProvider provider;
        private readonly IGrainFactory grainFactory;
        private const int HoldTime = 20 * 1000;
        private const int MonitTime = 60 * 2 * 1000;
        private const int checkTime = 10 * 1000;

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

        private const int lockHoldingSeconds = 60;
        private int distributedMonitorTimeLock = 0;
        private int distributedHoldTimerLock = 0;
        private int heathCheckTimerLock = 0;

        private async Task DistributedStart()
        {
            try
            {
                if (Interlocked.CompareExchange(ref this.distributedMonitorTimeLock, 1, 0) == 0)
                {
                    var consumers = this.rabbitEventBusContainer.GetConsumers();
                    foreach (var consumer in consumers)
                    {
                        if (consumer is RabbitConsumer value)
                        {
                            for (int i = 0; i < value.QueueList.Count(); i++)
                            {
                                var queue = value.QueueList[i];
                                var key = queue.ToString();
                                if (!this.Runners.ContainsKey(key))
                                {
                                    var weight = 100000 - this.Runners.Count;
                                    var (isOk, lockId, expectMillisecondDelay) = await this.grainFactory.GetGrain<IWeightHoldLock>(key).Lock(weight, lockHoldingSeconds);
                                    if (isOk)
                                    {
                                        if (this.Runners.TryAdd(key, lockId))
                                        {
                                            var runner = new ConsumerRunner(this.client, this.provider, value, queue);
                                            this.ConsumerRunners.TryAdd(key, runner);
                                            await runner.Run();
                                        }

                                    }
                                }
                            }
                        }
                    }

                    Interlocked.Exchange(ref this.distributedMonitorTimeLock, 0);
                    if (this.logger.IsEnabled(LogLevel.Information))
                    {
                        this.logger.LogInformation("EventBus Background Service is working.");
                    }
                }
            }
            catch (Exception exception)
            {
                this.logger.LogError(exception.InnerException ?? exception, nameof(this.DistributedStart));
                Interlocked.Exchange(ref this.distributedMonitorTimeLock, 0);
            }
        }

        private async Task DistributedHold()
        {
            try
            {
                if (this.logger.IsEnabled(LogLevel.Information))
                {
                    this.logger.LogInformation("EventBus Background Service is holding.");
                }

                if (Interlocked.CompareExchange(ref this.distributedHoldTimerLock, 1, 0) == 0)
                {
                    foreach (var lockKV in this.Runners)
                    {
                        if (this.Runners.TryGetValue(lockKV.Key, out var lockId))
                        {
                            var holdResult = await this.grainFactory.GetGrain<IWeightHoldLock>(lockKV.Key).Hold(lockId, lockHoldingSeconds);
                            if (!holdResult)
                            {
                                if (this.ConsumerRunners.TryRemove(lockKV.Key, out var runner))
                                {
                                    runner.Close();
                                }

                                this.Runners.TryRemove(lockKV.Key, out var _);
                            }
                        }
                    }

                    Interlocked.Exchange(ref this.distributedHoldTimerLock, 0);
                }
            }
            catch (Exception exception)
            {
                this.logger.LogError(exception.InnerException ?? exception, nameof(this.DistributedHold));
                Interlocked.Exchange(ref this.distributedHoldTimerLock, 0);
            }
        }

        private async Task HeathCheck()
        {
            try
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug("EventBus Background Service is checking.");
                }

                if (Interlocked.CompareExchange(ref this.heathCheckTimerLock, 1, 0) == 0)
                {
                    await Task.WhenAll(this.ConsumerRunners.Values.Select(runner => runner.HeathCheck()));
                    Interlocked.Exchange(ref this.heathCheckTimerLock, 0);
                }
            }
            catch (Exception exception)
            {
                this.logger.LogError(exception.InnerException ?? exception, nameof(this.HeathCheck));
                Interlocked.Exchange(ref this.heathCheckTimerLock, 0);
            }
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                this.logger.LogInformation("EventBus Background Service is starting.");
            }

            this.DistributedMonitorTime = new Timer(state => this.DistributedStart().Wait(), null, 1000, MonitTime);
            this.DistributedHoldTimer = new Timer(state => this.DistributedHold().Wait(), null, HoldTime, HoldTime);
            this.HeathCheckTimer = new Timer(state => { this.HeathCheck().Wait(); }, null, checkTime, checkTime);
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            if (this.logger.IsEnabled(LogLevel.Information))
            {
                this.logger.LogInformation("EventBus Background Service is stopping.");
            }

            this.Dispose();
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            if (this.logger.IsEnabled(LogLevel.Information))
            {
                this.logger.LogInformation("EventBus Background Service is disposing.");
            }

            foreach (var runner in this.ConsumerRunners.Values)
            {
                runner.Close();
            }

            this.DistributedMonitorTime?.Dispose();
            this.DistributedHoldTimer?.Dispose();
            this.HeathCheckTimer?.Dispose();
        }
    }
}
