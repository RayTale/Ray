using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Ray.Core.EventBus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.EventBus.RabbitMQ
{
    public class ConsumerRunner
    {
        readonly static TimeSpan while_TimeoutSpan = TimeSpan.FromMilliseconds(100);
        private bool isFirst = true;
        bool closed = false;
        public ConsumerRunner(
            IRabbitMQClient client,
            IServiceProvider provider,
            RabbitConsumer consumer,
            QueueInfo queue)
        {
            Client = client;
            Logger = provider.GetService<ILogger<ConsumerRunner>>();
            Consumer = consumer;
            Queue = queue;
        }
        public ILogger<ConsumerRunner> Logger { get; }
        public IRabbitMQClient Client { get; }
        public RabbitConsumer Consumer { get; }
        public QueueInfo Queue { get; }
        public ModelWrapper Model { get; set; }
        public bool IsUnAvailable => Model is null || Model.Model.IsClosed;

        public Task Run()
        {
            ThreadPool.UnsafeQueueUserWorkItem(async state =>
            {
                Model = Client.PullModel();
                if (isFirst)
                {
                    isFirst = false;
                    Model.Model.ExchangeDeclare(Consumer.EventBus.Exchange, "direct", true);
                    Model.Model.QueueDeclare(Queue.Queue, true, false, false, null);
                    Model.Model.QueueBind(Queue.Queue, Consumer.EventBus.Exchange, Queue.RoutingKey);
                }
                while (!closed)
                {
                    var list = new List<BytesBox>();
                    var batchStartTime = DateTimeOffset.UtcNow; ;
                    try
                    {
                        while (true)
                        {
                            var whileResult = Model.Model.BasicGet(Queue.Queue, Consumer.Config.AutoAck);
                            if (whileResult is null)
                            {
                                await Task.Delay(while_TimeoutSpan);
                                break;
                            }
                            else
                            {
                                list.Add(new BytesBox(whileResult.Body, whileResult));
                            }
                            if ((DateTimeOffset.UtcNow - batchStartTime).TotalMilliseconds > Model.Connection.Options.CunsumerMaxMillisecondsInterval ||
                            list.Count == Model.Connection.Options.CunsumerMaxBatchSize)
                            {
                                break;
                            }
                        }
                        if (list.Count > 0)
                            await Notice(list);
                    }
                    catch (Exception exception)
                    {
                        Logger.LogError(exception.InnerException ?? exception, $"An error occurred in {Queue}");
                        foreach (var item in list.Where(o => !o.Success))
                        {
                            Model.Model.BasicReject(((BasicGetResult)item.Origin).DeliveryTag, true);
                        }

                    }
                    finally
                    {
                        if (list.Count > 0)
                        {
                            Model.Model.BasicAck(list.Max(o => ((BasicGetResult)o.Origin).DeliveryTag), true);
                        }
                    }
                }
            }, null);
            return Task.CompletedTask;
        }
        async Task Notice(List<BytesBox> list, int times = 0)
        {
            try
            {
                if (list.Count > 1)
                {
                    await Consumer.Notice(list);
                }
                else if (list.Count == 1)
                {
                    await Consumer.Notice(list[0]);
                }
            }
            catch
            {
                if (Consumer.Config.RetryCount >= times)
                {
                    await Task.Delay(Consumer.Config.RetryIntervals);
                    await Notice(list.Where(o => !o.Success).ToList(), times + 1);
                }
                else
                    throw;
            }
        }
        public Task HeathCheck()
        {
            if (IsUnAvailable)
            {
                Close();
                return Run();
            }
            else
                return Task.CompletedTask;
        }
        public void Close()
        {
            closed = true;
            Model?.Dispose();
        }
    }
}
