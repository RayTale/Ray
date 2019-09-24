using RabbitMQ.Client.Events;

namespace Ray.EventBus.RabbitMQ
{
    public class ConsumerRunnerSlice
    {
        public ModelWrapper Channel { get; set; }
        public EventingBasicConsumer BasicConsumer { get; set; }
        public ushort Qos { get; set; }
        public bool NeedRestart { get; set; }
        public bool IsUnAvailable => NeedRestart || !BasicConsumer.IsRunning || Channel.Model.IsClosed;
        public void Close()
        {
            Channel?.Dispose();
        }
    }
}
