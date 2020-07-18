namespace Ray.EventBus.Kafka
{
    public class RayKafkaOptions
    {
        /// <summary>
        /// 消费者最大连接池
        /// </summary>
        public int ConsumerMaxPoolSize { get; set; } = 100;

        /// <summary>
        /// 生产者最大连接池
        /// </summary>
        public int ProducerMaxPoolSize { get; set; } = 100;

        /// <summary>
        /// 消费者批量处理每次处理的最大消息量
        /// </summary>
        public int CunsumerMaxBatchSize { get; set; } = 3000;

        /// <summary>
        /// 消费者批量处理每次处理的最大延时
        /// </summary>
        public int CunsumerMaxMillisecondsInterval { get; set; } = 1000;
    }
}
