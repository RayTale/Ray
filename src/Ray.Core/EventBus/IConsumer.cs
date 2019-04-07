using System.Threading.Tasks;

namespace Ray.Core.EventBus
{
    public interface IConsumer
    {
        string EventBusName { get; set; }
        Task Notice(byte[] bytes);
    }
}
