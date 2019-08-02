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
    }
}
