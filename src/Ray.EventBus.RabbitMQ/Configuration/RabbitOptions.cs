using System.Collections.Generic;
using RabbitMQ.Client;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitOptions
    {
        public string UserName { get; set; }
        public string Password { get; set; }
        public string VirtualHost { get; set; }
        public int PoolSizePerConnection { get; set; } = 200;
        public int MaxConnection { get; set; } = 20;
        /// <summary>
        /// 消费者批量处理每次处理的最大消息量
        /// </summary>
        public ushort CunsumerMaxBatchSize { get; set; } = 3000;
        /// <summary>
        /// 消费者批量处理每次处理的最大延时
        /// </summary>
        public int CunsumerMaxMillisecondsInterval { get; set; } = 1000;
        public string[] Hosts
        {
            get; set;
        }
        public List<AmqpTcpEndpoint> EndPoints
        {
            get
            {
                var list = new List<AmqpTcpEndpoint>();
                foreach (var host in Hosts)
                {
                    list.Add(AmqpTcpEndpoint.Parse(host));
                }
                return list;
            }
        }
    }
}
