using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;

namespace Ray.EventBus.Kafka
{
    public class ConsumerPooledObjectPolicy : IPooledObjectPolicy<PooledConsumer>
    {
        private readonly ConsumerConfig consumerConfig;
        private readonly ILogger logger;

        public ConsumerPooledObjectPolicy(ConsumerConfig consumerConfig, ILogger logger)
        {
            this.consumerConfig = consumerConfig;
            this.logger = logger;
        }

        public PooledConsumer Create()
        {
            return new PooledConsumer
            {
                Handler = new ConsumerBuilder<string, byte[]>(this.consumerConfig).SetErrorHandler(this.ConsumerClient_OnConsumeError).Build()
            };
        }

        private void ConsumerClient_OnConsumeError(IConsumer<string, byte[]> consumer, Error e)
        {
            this.logger.LogCritical("An error occurred during connect kafka(consumer) -->reason:{0},code:{1}", e.Reason, e.Code.ToString());
        }

        public bool Return(PooledConsumer obj)
        {
            return true;
        }
    }
}
