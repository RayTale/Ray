using System.Threading.Tasks;

namespace Ray.Core.Messaging.Channels
{
    public interface IMpscChannel<T, R> : IMpscChannelBase
    {
        Task<R> WriteAsync(T data);
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
