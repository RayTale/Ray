namespace Ray.EventBus.RabbitMQ
{
    /// <summary>
    /// Eventbus的branch配置
    /// </summary>
    public class BranchOptions
    {
        /// <summary>
        /// 最小qos
        /// </summary>
        public ushort MinQos { get; set; }
        /// <summary>
        /// 每次调整增加的qos
        /// </summary>
        public ushort IncQos { get; set; }
        /// <summary>
        /// 最大qos
        /// </summary>
        public ushort MaxQos { get; set; }
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
