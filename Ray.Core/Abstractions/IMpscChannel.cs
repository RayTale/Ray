using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core.Abstractions
{
    public interface IMpscChannel<T> : IMpscChannelBase
    {
        IMpscChannel<T> BindConsumer(Func<List<T>, Task> consumer);
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
