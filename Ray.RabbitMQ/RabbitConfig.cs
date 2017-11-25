using RabbitMQ.Client;
using System.Collections.Generic;

namespace Ray.RabbitMQ
{
    public class RabbitConfig
    {
        public string UserName { get; set; }
        public string Password { get; set; }
        public string VirtualHost { get; set; }
        public int MaxPoolSize { get; set; }
        public string[] Hosts
        {
            get
            {
                return hosts;
            }
            set
            {
                hosts = value;
                if (hosts != null)
                {
                    list = new List<AmqpTcpEndpoint>();
                    foreach (var host in hosts)
                    {
                        list.Add(AmqpTcpEndpoint.Parse(host));
                    }
                }
            }
        }
        List<AmqpTcpEndpoint> list;
        string[] hosts;
        public List<AmqpTcpEndpoint> EndPoints
        {
            get
            {
                return list;
            }
        }
    }
}
