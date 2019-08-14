﻿using System.Collections.Generic;
using RabbitMQ.Client;

namespace Ray.EventBus.RabbitMQ
{
    public class RabbitOptions
    {
        public string UserName { get; set; }
        public string Password { get; set; }
        public string VirtualHost { get; set; }
        public int MaxPoolSize { get; set; }
        public string[] Hosts
        {
            get;set;
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
