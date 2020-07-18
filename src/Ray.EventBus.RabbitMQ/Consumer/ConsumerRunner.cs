using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Ray.Core.EventBus;

namespace Ray.EventBus.RabbitMQ
{
    public class ConsumerRunner
    {
        private static readonly TimeSpan whileTimeoutSpan = TimeSpan.FromMilliseconds(100);
        private bool isFirst = true;
        private bool closed = false;

        public ConsumerRunner(
            IRabbitMQClient client,
            IServiceProvider provider,
            RabbitConsumer consumer,
            QueueInfo queue)
        {
            this.Client = client;
            this.Logger = provider.GetService<ILogger<ConsumerRunner>>();
            this.Consumer = consumer;
            this.Queue = queue;
        }

        public ILogger<ConsumerRunner> Logger { get; }

        public IRabbitMQClient Client { get; }

        public RabbitConsumer Consumer { get; }

        public QueueInfo Queue { get; }

        public ModelWrapper Model { get; set; }

        public bool IsUnAvailable => this.Model is null || this.Model.Model.IsClosed;

        public Task Run()
        {
            ThreadPool.UnsafeQueueUserWorkItem(
                async state =>
            {
                this.Model = this.Client.PullModel();
                if (this.isFirst)
                {
                    this.isFirst = false;
                    this.Model.Model.ExchangeDeclare(this.Consumer.EventBus.Exchange, "direct", true);
                    this.Model.Model.QueueDeclare(this.Queue.Queue, true, false, false, null);
                    this.Model.Model.QueueBind(this.Queue.Queue, this.Consumer.EventBus.Exchange, this.Queue.RoutingKey);
                }

                while (!this.closed)
                {
                    var list = new List<BytesBox>();
                    var batchStartTime = DateTimeOffset.UtcNow;
                    try
                    {
                        while (true)
                        {
                            var whileResult = this.Model.Model.BasicGet(this.Queue.Queue, this.Consumer.Config.AutoAck);
                            if (whileResult is null)
                            {
                                break;
                            }
                            else
                            {
                                list.Add(new BytesBox(whileResult.Body, whileResult));
                            }

                            if ((DateTimeOffset.UtcNow - batchStartTime).TotalMilliseconds > this.Model.Connection.Options.CunsumerMaxMillisecondsInterval ||
                            list.Count == this.Model.Connection.Options.CunsumerMaxBatchSize)
                            {
                                break;
                            }
                        }

                        if (list.Count > 0)
                        {
                            await this.Notice(list);
                        }
                        else
                        {
                            await Task.Delay(whileTimeoutSpan);
                        }
                    }
                    catch (Exception exception)
                    {
                        this.Logger.LogError(exception.InnerException ?? exception, $"An error occurred in {this.Queue}");
                        foreach (var item in list.Where(o => !o.Success))
                        {
                            this.Model.Model.BasicReject(((BasicGetResult)item.Origin).DeliveryTag, true);
                        }

                    }
                    finally
                    {
                        if (list.Count > 0)
                        {
                            this.Model.Model.BasicAck(list.Max(o => ((BasicGetResult)o.Origin).DeliveryTag), true);
                        }
                    }
                }
            }, null);
            return Task.CompletedTask;
        }

        private async Task Notice(List<BytesBox> list, int times = 0)
        {
            try
            {
                if (list.Count > 1)
                {
                    await this.Consumer.Notice(list);
                }
                else if (list.Count == 1)
                {
                    await this.Consumer.Notice(list[0]);
                }
            }
            catch
            {
                if (this.Consumer.Config.RetryCount >= times)
                {
                    await Task.Delay(this.Consumer.Config.RetryIntervals);
                    await this.Notice(list.Where(o => !o.Success).ToList(), times + 1);
                }
                else
                {
                    throw;
                }
            }
        }

        public Task HeathCheck()
        {
            if (this.IsUnAvailable)
            {
                this.Close();
                return this.Run();
            }
            else
            {
                return Task.CompletedTask;
            }
        }

        public void Close()
        {
            this.closed = true;
            this.Model?.Dispose();
        }
    }
}
