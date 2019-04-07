using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ray.Core.Channels
{
    public interface IMpscChannel<T> : IBaseMpscChannel
    {
        IMpscChannel<T> BindConsumer(Func<List<T>, Task> consumer);
        ValueTask<bool> WriteAsync(T data);
    }
}
