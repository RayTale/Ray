﻿namespace Ray.EventBus.RabbitMQ
{
    public class QueueInfo
    {
        public string Queue { get; set; }

        public string RoutingKey { get; set; }

        public override string ToString()
        {
            return $"{this.Queue}_{this.RoutingKey}";
        }
    }
}
