using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core.Messaging.Channels
{
    public interface IMpscChannel<T> : IMpscChannelBase
    {
        MpscChannel<T> BindConsumer(Func<List<T>, Task> consumer);
        ValueTask<bool> WriteAsync(T data);
    }
    public interface IMpscChannelBase
    {
        void JoinConsumerSequence(IMpscChannelBase channel);
        Task<bool> WaitToReadAsync();
        void ActiveConsumer();
        Task Consume();
        bool IsComplete { get; }
        void Complete();
    }
}
