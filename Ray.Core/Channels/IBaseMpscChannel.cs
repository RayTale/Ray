using System.Threading.Tasks;

namespace Ray.Core.Channels
{
    public interface IBaseMpscChannel
    {
        void JoinConsumerSequence(IBaseMpscChannel channel);
        Task<bool> WaitToReadAsync();
        void ActiveConsumer();
        Task Consume();
        bool IsComplete { get; }
        void Complete();
    }
}
