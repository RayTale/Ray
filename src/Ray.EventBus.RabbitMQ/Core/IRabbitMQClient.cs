namespace Ray.EventBus.RabbitMQ
{
    public interface IRabbitMQClient
    {
        ModelWrapper PullModel();
    }
}
