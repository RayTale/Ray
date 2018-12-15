using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Ray.Core.Messaging.Channels
{
    public interface IMpscChannelFactory<K, T, R> : IDisposable
    {
        IMpscChannel<T, R> Create(ILogger logger, K key, Func<List<MessageTaskWrapper<T, R>>, Task> consumer, int maxPerBatch = 5000);
    }
}
