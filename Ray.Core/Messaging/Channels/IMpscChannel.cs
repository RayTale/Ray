using System.Threading.Tasks;

namespace Ray.Core.Messaging.Channels
{
    public interface IMpscChannel<T> : IMpscChannelBase
    {
        ValueTask<bool> WriteAsync(T data);
    }
    public interface IMpscChannelBase
    {
        void JoinConsumerSequence(IMpscChannelBase channel);
        Task<bool> WaitToReadAsync();
        bool ActiveConsumer();
        Task Consume();
        bool IsComplete { get; }
        void Complete();
    }
}
