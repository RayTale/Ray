namespace Ray.EventBus.RabbitMQ
{
    /// <summary>
    /// Consumer配置信息
    /// </summary>
    public class ConsumerOptions
    {
        /// <summary>
        /// 是否自动ack
        /// </summary>
        public bool AutoAck { get; set; }
        /// <summary>
        /// 消息处理失败是否重回队列还是不停重发
        /// </summary>
        public bool Reenqueue { get; set; }
    }
}
