using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Prometheus.Client.MetricPusher;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.Metric.Prometheus
{
    public class MetricPushService : IHostedService, IDisposable
    {
        readonly ILogger<MetricPushService> logger;
        readonly MetricOption option;
        private MetricPushServer server;
        const int _checkTime = 30 * 1000;
        public MetricPushService(IOptions<MetricOption> option, ILogger<MetricPushService> logger)
        {
            this.option = option.Value;
            this.logger = logger;
        }
        private Timer HeathCheckTimer { get; set; }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var pusher = new MetricPusher(option.PushEndpoint, "ray-metric-push", this.option.ServiceName);
            server = new MetricPushServer(new IMetricPusher[]
           {
                pusher,
           });
            server.Start();
            HeathCheckTimer = new Timer(state => HeathCheck(), null, _checkTime, _checkTime);
            return Task.CompletedTask;
        }
        private void HeathCheck()
        {
            try
            {
                if (!server.IsRunning)
                    server.Start();
            }
            catch (Exception exception)
            {
                logger.LogError(exception.InnerException ?? exception, nameof(HeathCheck));
            }
        }
        public Task StopAsync(CancellationToken cancellationToken)
        {
            if (server.IsRunning)
                server.Stop();
            return Task.CompletedTask;
        }
        public void Dispose()
        {
            HeathCheckTimer.Dispose();
            if (server.IsRunning)
                server.Stop();
        }
    }
}
