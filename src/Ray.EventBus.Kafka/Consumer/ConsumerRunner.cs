using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Ray.Core.EventBus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.EventBus.Kafka
{
    public class ConsumerRunner
    {
        bool closed = false;
        readonly static TimeSpan while_TimeoutSpan = TimeSpan.FromMilliseconds(100);
        public ConsumerRunner(
            IKafkaClient client,
            ILogger<ConsumerRunner> logger,
            KafkaConsumer consumer,
            string topic)
        {
            Client = client;
            Logger = logger;
            Consumer = consumer;
            Topic = topic;
        }
        public ILogger<ConsumerRunner> Logger { get; }
        public IKafkaClient Client { get; }
        public KafkaConsumer Consumer { get; set; }
        public string Topic { get; }
        public Task Run()
        {
            ThreadPool.UnsafeQueueUserWorkItem(async state =>
            {
                using var consumer = Client.GetConsumer(Consumer.Group);
                consumer.Handler.Subscribe(Topic);
                while (!closed)
                {
                    var list = new List<BytesBox>();
                    var batchStartTime = DateTimeOffset.UtcNow;
                    try
                    {
                        while (true)
                        {
                            var whileResult = consumer.Handler.Consume(while_TimeoutSpan);
                            if (whileResult is null || whileResult.IsPartitionEOF || whileResult.Message.Value == null)
                            {
                                break;
                            }
                            else
                            {

                                list.Add(new BytesBox(whileResult.Message.Value, whileResult));
                            }
                            if ((DateTimeOffset.UtcNow - batchStartTime).TotalMilliseconds > consumer.MaxMillisecondsInterval || list.Count == consumer.MaxBatchSize) break;
                        }
                        await Notice(list);
                    }
                    catch (Exception exception)
                    {
                        Logger.LogError(exception.InnerException ?? exception, $"An error occurred in {Topic}");
                        using var producer = Client.GetProducer();
                        foreach (var item in list.Where(o => !o.Success))
                        {
                            var result = (ConsumeResult<string, byte[]>)item.Origin;
                            producer.Handler.Produce(Topic, new Message<string, byte[]> { Key = result.Message.Key, Value = result.Message.Value });
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
        public async Task HeathCheck()
        {
            if (!closed)
            {
                await Run();
            }
        }
        public void Close()
        {
            closed = true;
        }
    }
}
