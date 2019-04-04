namespace Ray.EventBus.RabbitMQ
{
    public class GroupsConfig
    {
        public GroupConfig[] Configs { get; set; }
    }
    public class GroupConfig
    {
        public string Group { get; set; }
        public ConsumerConfig Config { get; set; }
    }
}
