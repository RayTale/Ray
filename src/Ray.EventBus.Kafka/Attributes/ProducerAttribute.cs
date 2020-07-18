using System;

namespace Ray.EventBus.Kafka
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class ProducerAttribute : Attribute
    {
        public ProducerAttribute(string topic = default, int lBCount = 1, int retryCount = 3, int retryIntervals = 500)
        {
            this.Topic = topic;
            this.LBCount = lBCount;
            this.RetryCount = retryCount;
            this.RetryIntervals = retryIntervals;
        }

        public string Topic { get; }

        public int LBCount { get; }

        /// <summary>
        /// 发生异常重试次数
        /// </summary>
        public int RetryCount { get; set; }

        /// <summary>
        /// 重试间隔(ms)
        /// </summary>
        public int RetryIntervals { get; set; }
    }
}
