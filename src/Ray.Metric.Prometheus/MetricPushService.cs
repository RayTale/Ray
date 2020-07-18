using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Prometheus.Client.MetricPusher;

namespace Ray.Metric.Prometheus
{
    public class MetricPushService : IHostedService, IDisposable
    {
        private readonly ILogger<MetricPushService> logger;
        private readonly MetricOption option;
        private MetricPushServer server;
        private const int checkTime = 30 * 1000;

        public MetricPushService(IOptions<MetricOption> option, ILogger<MetricPushService> logger)
        {
            this.option = option.Value;
            this.logger = logger;
        }

        private Timer HeathCheckTimer { get; set; }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var pusher = new MetricPusher(this.option.PushEndpoint, "ray-metric-push", this.option.ServiceName);
            this.server = new MetricPushServer(new IMetricPusher[]
           {
                pusher,
           });
            this.server.Start();
            this.HeathCheckTimer = new Timer(state => this.HeathCheck(), null, checkTime, checkTime);
            return Task.CompletedTask;
        }

        private void HeathCheck()
        {
            try
            {
                if (!this.server.IsRunning)
                {
                    this.server.Start();
                }
            }
            catch (Exception exception)
            {
                this.logger.LogError(exception.InnerException ?? exception, nameof(this.HeathCheck));
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            if (this.server.IsRunning)
            {
                this.server.Stop();
            }

            return Task.CompletedTask;
        }

        public void Dispose()
        {
            this.HeathCheckTimer.Dispose();
            if (this.server.IsRunning)
            {
                this.server.Stop();
            }
        }
    }
}
