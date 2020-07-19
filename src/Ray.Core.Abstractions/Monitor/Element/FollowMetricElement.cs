namespace Ray.Core.Abstractions.Monitor
{
    public class FollowMetricElement
    {
        /// <summary>
        /// FollowActor的类型
        /// </summary>
        public string Actor { get; set; }

        /// <summary>
        /// Follow所属的Group
        /// </summary>
        public string Group { get; set; }

        /// <summary>
        /// 归属的Actor的类型
        /// </summary>
        public string FromActor { get; set; }

        /// <summary>
        /// 事件
        /// </summary>
        public string Event { get; set; }

        /// <summary>
        /// 事件送达的耗时(ms)
        /// </summary>
        public int DeliveryElapsedMs { get; set; }

        /// <summary>
        /// 执行耗时
        /// </summary>
        public int ElapsedMs { get; set; }
    }
}
