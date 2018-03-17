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
        string[] _hosts;
        public string[] Hosts
        {
            get
            {
                return _hosts;
            }
            set
            {
                _hosts = value;
                if (_hosts != null)
                {
                    List = new List<AmqpTcpEndpoint>();
                    foreach (var host in _hosts)
                    {
                        List.Add(AmqpTcpEndpoint.Parse(host));
                    }
                }
            }
        }
        public List<AmqpTcpEndpoint> EndPoints
        {
            get
            {
                return List;
            }
        }

        public List<AmqpTcpEndpoint> List { get; set; }
    }
}
