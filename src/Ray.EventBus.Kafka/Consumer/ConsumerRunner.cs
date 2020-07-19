using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Ray.Core.EventBus;

namespace Ray.EventBus.Kafka
{
    public class ConsumerRunner
    {
        private bool closed = false;
        private static readonly TimeSpan whileTimeoutSpan = TimeSpan.FromMilliseconds(100);

        public ConsumerRunner(
            IKafkaClient client,
            ILogger<ConsumerRunner> logger,
            KafkaConsumer consumer,
            string topic)
        {
            this.Client = client;
            this.Logger = logger;
            this.Consumer = consumer;
            this.Topic = topic;
        }

        public ILogger<ConsumerRunner> Logger { get; }

        public IKafkaClient Client { get; }

        public KafkaConsumer Consumer { get; set; }

        public string Topic { get; }

        public Task Run()
        {
            ThreadPool.UnsafeQueueUserWorkItem(
                async state =>
            {
                using var consumer = this.Client.GetConsumer(this.Consumer.Group);
                consumer.Handler.Subscribe(this.Topic);
                while (!this.closed)
                {
                    var list = new List<BytesBox>();
                    var batchStartTime = DateTimeOffset.UtcNow;
                    try
                    {
                        while (true)
                        {
                            var whileResult = consumer.Handler.Consume(whileTimeoutSpan);
                            if (whileResult is null || whileResult.IsPartitionEOF || whileResult.Message.Value == null)
                            {
                                break;
                            }
                            else
                            {
                                list.Add(new BytesBox(whileResult.Message.Value, whileResult));
                            }

                            if ((DateTimeOffset.UtcNow - batchStartTime).TotalMilliseconds > consumer.MaxMillisecondsInterval || list.Count == consumer.MaxBatchSize)
                            {
                                break;
                            }
                        }

                        await this.Notice(list);
                    }
                    catch (Exception exception)
                    {
                        this.Logger.LogError(exception.InnerException ?? exception, $"An error occurred in {this.Topic}");
                        using var producer = this.Client.GetProducer();
                        foreach (var item in list.Where(o => !o.Success))
                        {
                            var result = (ConsumeResult<string, byte[]>)item.Origin;
                            producer.Handler.Produce(this.Topic, new Message<string, byte[]> { Key = result.Message.Key, Value = result.Message.Value });
                        }
                    }
                    finally
                    {
                        if (list.Count > 0)
                        {
                            consumer.Handler.Commit();
                        }
                    }
                }

                consumer.Handler.Unsubscribe();
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

        public async Task HeathCheck()
        {
            if (!this.closed)
            {
                await this.Run();
            }
        }

        public void Close()
        {
            this.closed = true;
        }
    }
}
