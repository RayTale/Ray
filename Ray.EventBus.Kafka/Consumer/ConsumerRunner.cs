using Microsoft.Extensions.Logging;
using System;
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
        Task runningTask;
        public Task Run()
        {
            runningTask = Task.Factory.StartNew(async () =>
            {
                using (var consumer = Client.GetConsumer(Consumer.Group))
                {
                    consumer.Handler.Subscribe(Topic);
                    while (!closed)
                    {
                        try
                        {
                            IsHeath = true;
                            var data = consumer.Handler.Consume();
                            await Consumer.Notice(data.Value);
                        }
                        catch (Exception exception)
                        {
                            Logger.LogError(exception.InnerException ?? exception, $"An error occurred in {Topic}");
                        }
                    }
                    IsHeath = false;
                    consumer.Handler.Unsubscribe();
                }
                runningTask.Dispose();
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
