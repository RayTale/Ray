using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
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
            string topic,
            bool reenqueue = true)
        {
            Client = client;
            Logger = logger;
            Consumer = consumer;
            Topic = topic;
            Reenqueue = reenqueue;
        }
        public ILogger<ConsumerRunner> Logger { get; }
        public IKafkaClient Client { get; }
        public KafkaConsumer Consumer { get; set; }
        public string Topic { get; }
        public bool Reenqueue { get; }
        public DateTimeOffset StartTime { get; set; }
        bool IsHeath = true;
        bool closed = false;
        readonly static TimeSpan start_TimeoutSpan = TimeSpan.FromSeconds(30);
        readonly static TimeSpan while_TimeoutSpan = TimeSpan.FromMilliseconds(100);
        DateTimeOffset lastCommitTime = DateTimeOffset.UtcNow;
        public Task Run()
        {
            ThreadPool.QueueUserWorkItem(async state =>
            {
                IsHeath = true;
                using var consumer = Client.GetConsumer(Consumer.Group);
                consumer.Handler.Subscribe(Topic);
                bool needCommit = false;
                while (!closed)
                {
                    var consumerResult = consumer.Handler.Consume(start_TimeoutSpan);
                    if (consumerResult is null || consumerResult.IsPartitionEOF || consumerResult.Value == null)
                    {
                        if (needCommit)
                        {
                            consumer.Handler.Commit();
                            needCommit = false;
                            lastCommitTime = DateTimeOffset.UtcNow;
                        }
                        continue;
                    }
                    List<ConsumeResult<string, byte[]>> list = default;
                    DateTimeOffset batchStartTime = default;
                    while (true)
                    {
                        var whileResult = consumer.Handler.Consume(while_TimeoutSpan);
                        if (whileResult is null || whileResult.IsPartitionEOF || whileResult.Value == null)
                        {
                            break;
                        }
                        else
                        {
                            if (list == null)
                            {
                                list = new List<ConsumeResult<string, byte[]>>();
                                batchStartTime = DateTimeOffset.UtcNow;
                            }
                            list.Add(whileResult);
                        }
                        if ((DateTimeOffset.UtcNow - batchStartTime).TotalMilliseconds > consumer.MaxMillisecondsInterval || list.Count == consumer.MaxBatchSize) break;
                    }
                    try
                    {
                        if (list == null)
                            await Consumer.Notice(consumerResult.Value);
                        else
                        {
                            list.Add(consumerResult);
                            await Consumer.Notice(list.Select(o => o.Value).ToList());
                        }
                    }
                    catch (Exception exception)
                    {
                        Logger.LogError(exception.InnerException ?? exception, $"An error occurred in {Topic}");
                        if (Reenqueue)
                        {
                            await Task.Delay(1000);
                            using var producer = Client.GetProducer();
                            if (list == null)
                                producer.Handler.Produce(Topic, new Message<string, byte[]> { Key = consumerResult.Key, Value = consumerResult.Value });
                            else
                            {
                                foreach (var item in list)
                                {
                                    producer.Handler.Produce(Topic, new Message<string, byte[]> { Key = item.Key, Value = item.Value });
                                }
                            }
                        }
                        else
                        {
                            IsHeath = false;
                        }
                    }
                    finally
                    {
                        if (IsHeath)
                        {
                            var nowTime = DateTimeOffset.UtcNow;
                            if (list != null || (nowTime - lastCommitTime).TotalSeconds > 1)
                            {
                                consumer.Handler.Commit();
                                needCommit = false;
                                lastCommitTime = nowTime;
                            }
                            else
                            {
                                needCommit = true;
                            }
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
