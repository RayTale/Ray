using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.EventBus.Kafka
{
    public class ConsumerRunner
    {
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
        public DateTimeOffset StartTime { get; set; }
        bool IsHeath = true;
        bool closed = false;
        static TimeSpan timeoutTime = TimeSpan.FromSeconds(30);
        DateTimeOffset lastCommitTime=DateTimeOffset.UtcNow;
        public Task Run()
        {
            ThreadPool.QueueUserWorkItem(async state =>
            {
                using var consumer = Client.GetConsumer(Consumer.Group);
                consumer.Handler.Subscribe(Topic);
                bool needCommit = false;
                while (!closed)
                {
                    var consumerResult = consumer.Handler.Consume(timeoutTime);
                    if (consumerResult == default || consumerResult.IsPartitionEOF || consumerResult.Value == null)
                    {
                        if (needCommit)
                        {
                            consumer.Handler.Commit();
                            needCommit = false;
                            lastCommitTime = DateTimeOffset.UtcNow;
                        }
                        continue;
                    }
                    try
                    {
                        IsHeath = true;
                        await Consumer.Notice(consumerResult.Value);
                    }
                    catch (Exception exception)
                    {
                        Logger.LogError(exception.InnerException ?? exception, $"An error occurred in {Topic}");
                        using var producer = Client.GetProducer();
                        producer.Handler.Produce(Topic, new Message<string, byte[]> { Key = consumerResult.Key, Value = consumerResult.Value });
                    }
                    finally
                    {
                        var nowTime = DateTimeOffset.UtcNow;
                        if ((nowTime - lastCommitTime).TotalSeconds>1)
                        {
                            consumer.Handler.Commit(consumerResult);
                            needCommit = false;
                            lastCommitTime = nowTime;
                        }
                        else
                        {
                            needCommit = true;
                        }
                    }
                }
                IsHeath = false;
                consumer.Handler.Unsubscribe();
            });
            return Task.CompletedTask;
        }
        public async Task HeathCheck()
        {
            if (!IsHeath && !closed)
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
