namespace Ray.EventBus.Kafka
{
    public interface IKafkaClient
    {
        PooledConsumer GetConsumer(string group);

        PooledProducer GetProducer();
    }
}
