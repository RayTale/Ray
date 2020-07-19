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
        /// 发生异常重试次数
        /// </summary>
        public int RetryCount { get; set; } = 3;

        /// <summary>
        /// 重试间隔(ms)
        /// </summary>
        public int RetryIntervals { get; set; } = 500;
    }
}
